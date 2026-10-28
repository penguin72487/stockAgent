from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import fcntl
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import download_finmind_complement as complement


@pytest.fixture
def admitted_capacity(tmp_path):
    """Keep scheduling tests independent of the machine's real disk headroom."""
    from types import SimpleNamespace
    return SimpleNamespace(root=tmp_path.absolute(),
                           initial_sample=lambda: {'state': 'admitted'},
                           sample=lambda: {'state': 'admitted'})


def test_physical_capacity_pause_preserves_source_counts_without_credentials(tmp_path, monkeypatch):
    from types import SimpleNamespace
    previous = {'state': 'batch_complete', 'observed_at_utc': '2026-01-01T00:00:00+00:00',
                'series': {'test': {'rows': 123}}, 'last_task': {'dataset': 'test'}}
    (tmp_path / 'status.json').write_text(json.dumps(previous))
    admission = {'state': 'deferred', 'reason': 'physical_backing_reserve'}
    sampler = SimpleNamespace(root=tmp_path.absolute(), initial_sample=lambda: admission)
    monkeypatch.setattr(complement, 'load_env_file', lambda *_a, **_k: pytest.fail('credential read'))
    result = complement.run_once(tmp_path, capacity_sampler=sampler)
    assert result['state'] == 'disk_guard' and result['capacity_admission'] == admission
    assert result['dispatch_contract_version'] == complement.DISPATCH_CONTRACT_VERSION
    assert result['series'] == previous['series'] and result['last_task'] == previous['last_task']
    assert result['source_status_observed_at_utc'] == previous['observed_at_utc']


def test_physical_capacity_sampler_cannot_be_shared_with_a_different_root(tmp_path):
    from types import SimpleNamespace
    with pytest.raises(ValueError, match='different workspace'):
        complement.run_once(tmp_path / 'source', capacity_sampler=SimpleNamespace(root=tmp_path / 'other'))


def test_capacity_wait_rechecks_each_minute_and_reuses_sampler(tmp_path, monkeypatch):
    from types import SimpleNamespace
    sampler = SimpleNamespace(root=tmp_path.absolute())
    monkeypatch.setattr(complement, 'AcquisitionCapacitySampler', lambda _root: sampler)
    seen, delays = [], []
    def cycle(_root, **kwargs):
        seen.append(kwargs['capacity_sampler'])
        if len(seen) == 3:
            raise KeyboardInterrupt
        return {'state': 'disk_guard'}
    monkeypatch.setattr(complement, 'run_once', cycle)
    monkeypatch.setattr(complement, 'wait_for_next_cycle', lambda _r, _s, delay, **_k: delays.append(delay))
    assert complement.main(['--root', str(tmp_path), '--loop']) == 130
    assert seen == [sampler, sampler, sampler] and delays == [60, 60]


def test_exact_key_cli_holds_shared_worker_lock(tmp_path: Path, monkeypatch) -> None:
    calls = []

    def run(root, **kwargs):
        with (root / "worker.lock").open("a+") as competing:
            with pytest.raises(BlockingIOError):
                fcntl.flock(competing.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls.append(kwargs)
        return {"state": "current_queue"}

    monkeypatch.setattr(complement, "run_once", run)
    assert complement.main(["--root", str(tmp_path), "--max-requests", "2", "--task-key",
                            "TaiwanFuturesKBar", "CR1", "2012-08-22"]) == 0
    assert calls == [{"max_requests": 2, "datasets": None,
                      "required_keys": (("TaiwanFuturesKBar", "CR1", "2012-08-22"),)}]


def test_cli_default_quantum_uses_the_same_fingerprinted_configuration(tmp_path, monkeypatch):
    from downloader import finmind_json_pipeline
    calls = []
    monkeypatch.setattr(complement, 'run_once', lambda root, **kw: calls.append(kw) or {'state': 'current_queue'})
    monkeypatch.setattr(complement.object_pipeline, 'initialize_cpu', lambda threads: None)
    assert complement.main(['--root', str(tmp_path)]) == 0
    assert calls[0]['max_requests'] == finmind_json_pipeline.settings()['max_requests_per_cycle']


def test_exact_key_cli_rejects_competing_owner_before_provider_call(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(complement, "run_once", lambda *a, **k: pytest.fail("competing provider call"))
    with (tmp_path / "worker.lock").open("a+") as active:
        fcntl.flock(active.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert complement.main(["--root", str(tmp_path), "--task-key", "TaiwanFuturesKBar",
                                "CJ1", "2015-08-13"]) == 2


@pytest.mark.parametrize("extra", [["--loop"], ["--retry-blocked"], ["--dataset", "TaiwanFuturesKBar"]])
def test_exact_key_cli_cannot_expand_finite_scope(tmp_path: Path, monkeypatch, extra) -> None:
    monkeypatch.setattr(complement, "run_once", lambda *a, **k: pytest.fail("invalid finite scope"))
    with pytest.raises(SystemExit) as error:
        complement.main(["--root", str(tmp_path), "--task-key", "TaiwanFuturesKBar", "CR1",
                         "2012-08-22", *extra])
    assert error.value.code == 2


def test_complement_catalog_includes_all_sponsor_per_id_sources_and_news() -> None:
    assert len(complement.ALL_DATASETS) == 69
    assert len(set(complement.ALL_DATASETS)) == 69
    assert "TaiwanStockNews" in complement.ALL_DATASETS
    assert "TaiwanStockPriceTick" in complement.ALL_DATASETS
    assert "TaiwanStockKBar" in complement.ALL_DATASETS
    assert complement.supplemental.SOURCES['TaiwanStockPriceTick'].priority > complement.supplemental.SOURCES['TaiwanStockKBar'].priority


def test_live_sponsor_delegates_duplicate_finmind_tasks_and_stale_state_falls_back(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, tzinfo=UTC)
    root = tmp_path / "data_finmind" / "complement"
    sponsor = root.parent / "sponsor"
    sponsor.mkdir(parents=True)
    (sponsor / "status.json").write_text(json.dumps({
        "tier": "Sponsor", "state": "running", "observed_at_utc": now.isoformat(),
        "series": {"TaiwanStockPrice": {"target": 100, "blocked": 0}},
    }))
    with complement._db(root / "queue.sqlite3") as connection:
        complement._add_tasks(connection, [
            ("TaiwanStockPrice", "2330", "history", "id_history", 1),
            ("TaiwanExchangeRate", "USD", "history", "id_history", 2),
        ])
        delegated = complement._sponsor_delegated(root, now)
        assert delegated == frozenset({"TaiwanStockPrice"})
        assert complement._next_task(connection, now, delegated=delegated).dataset == "TaiwanExchangeRate"
        assert not complement._sponsor_delegated(root, now + timedelta(minutes=16))
        assert complement._next_task(connection, now + timedelta(minutes=16)).dataset == "TaiwanStockPrice"


def test_queue_has_all_reference_and_historical_ranges(tmp_path: Path) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        count = connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        # Newly proven market-wide SpreadTick does not require a master ID,
        # and is still a bounded day queue, not one full-history request.
        market_tick_days = connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanFuturesSpreadTick' AND data_id=''"
        ).fetchone()[0]
        assert 0 < market_tick_days <= complement.supplemental.WORKING_SET
        news_days = (date(2026, 9, 25) - complement.news.SEARCH_FLOOR).days + 1
        assert count == (news_days + len(complement.SNAPSHOTS) - len(complement.DERIVATIVE_SNAPSHOTS) + sum(2026 - first + 1 for first in complement.GLOBAL_START_YEAR.values())
                         + sum(len(ids) for ids in complement.FIXED_ID_HISTORY.values())
                         + len(complement.CURRENCIES) + len(complement.supplemental.MARKET_HISTORY_DATASETS)
                         + market_tick_days)
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockNews'").fetchone()[0] == news_days
        assert connection.execute("SELECT priority FROM tasks WHERE dataset='GoldPrice' AND partition='1900'").fetchone()[0] == 3
        assert connection.execute("SELECT priority FROM tasks WHERE dataset='GoldPrice' AND partition='2014'").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanExchangeRate' AND data_id=''").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanExchangeRate'").fetchone()[0] == 19
        assert complement._next_task(connection, datetime(2026, 9, 25, tzinfo=UTC)) is not None


@pytest.mark.parametrize("state,age", [("incremental_reserve", 10), ("waiting_necessary_acquisition", 10),
                                       ("rate_limited", 31), ("ip_banned", 31)])
def test_shared_account_wait_does_not_duplicate_sponsor_work(tmp_path: Path, state: str, age: int) -> None:
    now = datetime(2026, 9, 26, tzinfo=UTC)
    root = tmp_path / "complement"
    path = tmp_path / "sponsor" / "status.json"
    path.parent.mkdir()
    path.write_text(json.dumps({
        "tier": "Sponsor", "state": state,
        "observed_at_utc": (now - timedelta(minutes=age)).isoformat(),
        "series": {"TaiwanStockPrice": {"target": 100, "blocked": 0}},
    }))
    assert complement._sponsor_delegated(root, now) == frozenset({"TaiwanStockPrice"})
    assert not complement._sponsor_delegated(root, now + timedelta(minutes=36))


def test_background_reserve_keeps_incremental_and_zero_api_derivation(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, tzinfo=UTC)
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._add_tasks(connection, [
            ("Historical", "2330", "history", "id_history", 1),
            ("Incremental", "", "2026", "year", 0),
            (complement.LONG_INSTITUTIONAL, "2330", "history", "id_history", 1),
            (complement.WIDE_INSTITUTIONAL, "2330", "history", "derived", 3),
        ])
        assert complement._next_task(connection, now, incremental_only=True).dataset == "Incremental"
        connection.execute("UPDATE tasks SET state='complete' WHERE dataset IN (?,?)",
                           ("Incremental", complement.LONG_INSTITUTIONAL))
        assert complement._next_task(connection, now, incremental_only=True).kind == "derived"


def test_due_repair_precedes_pending_in_same_lane_without_bypassing_cooldown(tmp_path: Path) -> None:
    now = datetime(2026, 10, 2, tzinfo=UTC)
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._add_tasks(connection, [
            ("TaiwanStockNews", "", "1900-01-01", "market_day", 6),
            ("TaiwanStockNews", "", "2013-05-03", "market_day", 6),
            ("TaiwanStockNews", "", "2026-10-02", "market_day", 0),
        ])
        connection.execute(
            "UPDATE tasks SET state='failed',next_attempt_at_utc=? WHERE partition='2013-05-03'",
            ((now + timedelta(minutes=15)).isoformat(),),
        )
        assert complement._next_task(connection, now).partition == "2026-10-02"
        assert complement._next_task(connection, now, background_only=True).partition == "1900-01-01"
        eligible = now + timedelta(minutes=16)
        assert complement._next_task(connection, eligible).partition == "2026-10-02"
        assert complement._next_task(connection, eligible, background_only=True).partition == "2013-05-03"
        assert complement._next_task(connection, eligible, incremental_only=True).partition == "2026-10-02"
        assert complement._next_task(connection, eligible,
                                     delegated=frozenset({"TaiwanStockNews"})) is None
        connection.execute("UPDATE tasks SET state='not_entitled' WHERE partition='2013-05-03'")
        assert complement._next_task(connection, eligible, background_only=True).partition == "1900-01-01"


@pytest.mark.parametrize('state',['complete','observed_empty','inflight','blocked','unsupported'])
def test_finite_accounting_keys_never_reopen_terminal_or_running_tasks(tmp_path,state):
    now=datetime(2026,10,1,tzinfo=UTC)
    key=('TaiwanFuturesKBar','CR1','2012-08-22')
    with complement._db(tmp_path/'queue.sqlite3') as connection:
        complement._add_tasks(connection,[(*key,'day',8),
            ('TaiwanFuturesKBar','TX','2026-09-30','day',8)])
        connection.execute('UPDATE tasks SET state=? WHERE data_id=?',(state,'CR1'))
        before=list(connection.execute('SELECT * FROM tasks ORDER BY data_id'))
        assert complement._next_task(connection,now,required_keys=(key,)) is None
        assert list(connection.execute('SELECT * FROM tasks ORDER BY data_id'))==before


def test_finite_accounting_keys_keep_quota_cooldown_and_owner_boundaries(tmp_path):
    now=datetime(2026,10,1,tzinfo=UTC)
    key=('TaiwanFuturesKBar','CR1','2012-08-22')
    with complement._db(tmp_path/'queue.sqlite3') as connection:
        complement._add_tasks(connection,[(*key,'day',8)])
        assert complement._next_task(connection,now,required_keys=(key,)).data_id=='CR1'
        assert complement._next_task(connection,now,required_keys=(key,),incremental_only=True) is None
        assert complement._next_task(connection,now,required_keys=(key,),delegated=frozenset({key[0]})) is None
        connection.execute('UPDATE tasks SET state=?,next_attempt_at_utc=?',
            ('failed',(now+timedelta(hours=1)).isoformat()))
        assert complement._next_task(connection,now,required_keys=(key,)) is None
        assert complement._next_task(connection,now+timedelta(hours=2),required_keys=(key,)) is not None


def test_finite_accounting_keys_validate_scope_before_loading_credentials(tmp_path):
    for keys in [(),(('not-a-provider-dataset','CR1','2012-08-22'),),
                 tuple(('TaiwanFuturesKBar',str(i),'2012-08-22') for i in range(65))]:
        with pytest.raises(ValueError,match='registered finite task keys'):
            complement.run_once(tmp_path,required_keys=keys)


def test_completed_bounded_batch_writes_status_before_closing_connection(tmp_path, monkeypatch, admitted_capacity) -> None:
    from types import SimpleNamespace

    now = datetime(2026, 9, 27, 8, tzinfo=UTC)
    monkeypatch.setenv("FINMIND_TOKEN", "test-only")
    monkeypatch.setattr(complement, "load_env_file", lambda *_a, **_kw: None)
    monkeypatch.setattr(complement, "_now", lambda: now)
    monkeypatch.setattr(complement, "verified_account", lambda *_a: {"tier": "Free", "official_requests_per_hour": 600})
    monkeypatch.setattr(complement, "rate_limiter", lambda *_a: object())
    monkeypatch.setattr(complement, "backfill_budget", lambda *_a, **_kw: {"allowed": True})
    monkeypatch.setattr(complement, "incremental_reservation", lambda *_a: {'reserve_requests': 0})
    monkeypatch.setattr(complement.shutil, "disk_usage", lambda *_a: SimpleNamespace(free=10**12))
    monkeypatch.setattr(complement, "_populate", lambda conn, *_a, **_kw: complement._add_tasks(
        conn, [("TaiwanStockInfo", "", "latest", "snapshot", 0)],
    ))
    monkeypatch.setattr(complement, "_request", lambda *_a, **_kw: [{"stock_id": "2330", "date": "2026-09-24"}])
    result = complement.run_once(tmp_path, max_requests=1, capacity_sampler=admitted_capacity)
    assert result["state"] == "batch_complete"
    assert result["series"]["TaiwanStockInfo"]["complete"] == 1


@pytest.mark.parametrize("keep_prepared,verified,remaining", [(True, True, 100), (False, True, 100),
                                                          (True, False, 100), (True, True, 0)])
def test_short_cross_lane_preemption_reuses_prepared_owner_without_reseeding(
        tmp_path, monkeypatch, admitted_capacity, keep_prepared, verified, remaining):
    from types import SimpleNamespace
    from downloader import finmind_integrity

    now = datetime(2026, 10, 17, 8, tzinfo=UTC)
    monkeypatch.setenv("FINMIND_TOKEN", "test-only")
    monkeypatch.setattr(complement, "load_env_file", lambda *_a, **_kw: None)
    monkeypatch.setattr(complement, "_now", lambda: now)
    monkeypatch.setattr(complement, "verified_account", lambda *_a: {"tier": "Free", "official_requests_per_hour": 600})
    monkeypatch.setattr(complement, "rate_limiter", lambda *_a: object())
    monkeypatch.setattr(complement.shutil, "disk_usage", lambda *_a: SimpleNamespace(free=10**12))
    ready, prepared, audited, fetched, waits = [True], [], [], [], []

    def populate(conn, *_a, **_kw):
        prepared.append(id(conn))
        complement._add_tasks(conn, [("TaiwanStockDelisting", "", "2025", "year", 4)])

    monkeypatch.setattr(complement, "_populate", populate)
    monkeypatch.setattr(finmind_integrity, "audit_completed_batch", lambda conn, *_a: audited.append(id(conn)))
    monkeypatch.setattr(complement, "incremental_reservation", lambda *_a: {"reserve_requests": int(ready[0])})
    monkeypatch.setattr(complement, "backfill_budget", lambda *_a, **_kw: {
        "allowed": not ready[0], "priority_wait": ready[0], "schedule_verified": verified,
        "remaining": remaining, "reserve": int(ready[0])})
    monkeypatch.setattr(complement, "_claim_bulk_years", lambda *_a, **_kw: None)

    def request(*_a, **_kw):
        assert not ready[0]  # No historical API call while a release owns priority.
        fetched.append(True)
        return [{"date": "2025-07-01", "stock_id": "1234"}]

    monkeypatch.setattr(complement, "_request", request)

    def wait(root, status, seconds, **kw):
        assert not fetched and status["active_task"] is None
        assert seconds == 60 and kw["wake_if"]() is False
        waits.append(status["state"])
        ready[0] = False

    monkeypatch.setattr(complement, "wait_for_next_cycle", wait)
    result = complement.run_once(tmp_path, max_requests=1, keep_prepared_on_priority_wait=keep_prepared,
                                 capacity_sampler=admitted_capacity)
    should_reuse = keep_prepared and verified and remaining > 1
    assert result["state"] == ("batch_complete" if should_reuse else "incremental_reserve")
    assert len(fetched) == len(waits) == int(should_reuse)
    assert len(prepared) == len(audited) == 1 and prepared == audited


def test_finite_key_run_fetches_only_selected_year_and_preserves_neighbour(tmp_path, monkeypatch, admitted_capacity) -> None:
    from types import SimpleNamespace
    from downloader import finmind_integrity

    now=datetime(2026,9,27,8,tzinfo=UTC)
    monkeypatch.setenv('FINMIND_TOKEN','test-only')
    monkeypatch.setattr(complement,'load_env_file',lambda *_a,**_k:None)
    monkeypatch.setattr(complement,'_now',lambda:now)
    monkeypatch.setattr(complement,'verified_account',lambda *_a:{'tier':'Free','official_requests_per_hour':600})
    monkeypatch.setattr(complement,'rate_limiter',lambda *_a:object())
    monkeypatch.setattr(complement,'backfill_budget',lambda *_a,**_k:{'allowed':True,'remaining':2})
    monkeypatch.setattr(complement,'incremental_reservation',lambda *_a:{'reserve_requests':0})
    monkeypatch.setattr(complement.shutil,'disk_usage',lambda *_a:SimpleNamespace(free=10**12))
    monkeypatch.setattr(complement,'_populate',lambda *_a,**_k:pytest.fail('unrelated queue population'))
    monkeypatch.setattr(complement,'_claim_bulk_years',lambda *_a,**_k:pytest.fail('widened year claim'))
    monkeypatch.setattr(finmind_integrity,'audit_completed_batch',lambda *_a,**_k:pytest.fail('unrelated receipt audit'))
    key=('TaiwanStockDelisting','','2025')
    with complement._db(tmp_path/'queue.sqlite3') as connection:
        complement._add_tasks(connection,[(*key,'year',4),('TaiwanStockDelisting','','2024','year',4)])
    requested=[]
    def request(_session,_limiter,_root,task,_token,**kwargs):
        requested.append((task.dataset,task.data_id,task.partition))
        assert not kwargs.get('full_history',False)
        return [{'date':'2025-07-01','stock_id':'1234'}]
    monkeypatch.setattr(complement,'_request',request)
    result=complement.run_once(tmp_path,max_requests=1,required_keys=(key,),capacity_sampler=admitted_capacity)
    assert result['state']=='batch_complete' and requested==[key]
    with complement._db(tmp_path/'queue.sqlite3') as connection:
        assert connection.execute("SELECT state FROM tasks WHERE partition='2025'").fetchone()[0]=='complete'
        assert connection.execute("SELECT state FROM tasks WHERE partition='2024'").fetchone()[0]=='pending'


def test_master_snapshot_expands_free_per_symbol_jobs(tmp_path: Path) -> None:
    snapshot = complement.Task("TaiwanStockInfo", "", "latest", "snapshot", 0, "pending")
    complement._store(tmp_path, snapshot, [
        {"date": "2026-09-25", "stock_id": "2330", "type": "twse"},
        {"date": "2026-09-25", "stock_id": "2317", "type": "twse"},
    ], datetime(2026, 9, 25, tzinfo=UTC))
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockPrice'"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockMonthRevenue'"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockCapitalReductionReferencePrice' "
            "AND kind='id_history'"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset=? AND kind='derived'",
            (complement.WIDE_INSTITUTIONAL,),
        ).fetchone()[0] == 2


def test_official_delisted_and_later_listings_expand_existing_jobs(tmp_path: Path) -> None:
    root = tmp_path / "data_finmind" / "complement"
    public = tmp_path / "data_tw_public"
    public.mkdir()
    pq.write_table(pa.Table.from_pylist([{"symbol": "1204"}]), public / "twse_delisted_company.parquet")
    snapshot = complement.Task("TaiwanStockInfo", "", "latest", "snapshot", 0, "pending")
    now = datetime(2026, 9, 25, tzinfo=UTC)
    complement._store(root, snapshot, [{"date": "2026-09-25", "stock_id": "2330"}], now)
    with complement._db(root / "queue.sqlite3") as connection:
        complement._populate(connection, root, today=date(2026, 9, 25))
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockPrice'").fetchone()[0] == 2
        complement._store(root, snapshot, [
            {"date": "2026-09-26", "stock_id": "2330"},
            {"date": "2026-09-26", "stock_id": "2317"},
        ], now)
        complement._populate(connection, root, today=date(2026, 9, 26))
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockPrice'").fetchone()[0] == 3


def test_entitlement_blocks_per_id_dataset_without_retries(tmp_path: Path) -> None:
    now = datetime(2026, 9, 25, tzinfo=UTC)
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        task = complement.Task("TaiwanExchangeRate", "USD", "history", "id_history", 1, "pending")
        complement._save_failure(connection, task, complement.SourceError("not_entitled", retry_after=86400), now)
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset=? AND state='not_entitled'",
            (task.dataset,),
        ).fetchone()[0] == 19
        due = connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset=? AND next_attempt_at_utc<?",
            (task.dataset, (now + timedelta(days=2)).isoformat()),
        ).fetchone()[0]
        assert due == 0


def test_current_snapshot_refreshes_at_next_taipei_14_and_current_year_in_four_hours() -> None:
    observed = datetime(2026, 9, 25, 15, 0, tzinfo=UTC)
    snapshot = complement.Task("TaiwanStockInfo", "", "latest", "snapshot", 0, "complete")
    current = complement.Task("GoldPrice", "", "2026", "year", 0, "complete")
    assert complement._next_refresh(snapshot, observed, empty=False) == "2026-09-26T06:00:00+00:00"
    assert complement._next_refresh(current, observed, empty=False) == "2026-09-25T19:00:00+00:00"
    margin = complement.Task("TaiwanStockTotalMarginPurchaseShortSale", "", "2026", "year", 0, "complete")
    assert complement._next_refresh(margin, observed, empty=False) == "2026-09-28T13:00:00+00:00"


def test_receipt_keeps_raw_dates_and_rejects_wrong_identity(tmp_path: Path) -> None:
    now = datetime(2026, 9, 25, 8, tzinfo=UTC)
    task = complement.Task("TaiwanStockPrice", "2330", "history", "id_history", 2, "pending")
    receipt = complement._store(tmp_path, task, [
        {"date": "2000-01-03", "stock_id": "2330", "close": 10},
        {"date": "2026-09-24", "stock_id": "2330", "close": 200},
    ], now)
    assert receipt["status"] == "complete"
    assert receipt["rows"] == 2
    assert receipt["source_first_date"] == "2000-01-03"
    assert receipt["source_last_date"] == "2026-09-24"
    assert (tmp_path / receipt["parquet_path"]).is_file()
    assert json.loads((tmp_path / receipt["receipt_path"]).read_text())["historical_point_in_time"] is False
    with pytest.raises(complement.SourceError, match="wrong_data_id"):
        complement._store(tmp_path, task, [{"date": "2020-01-01", "stock_id": "2317"}], now)
    year = complement.Task("GoldPrice", "", "2026", "year", 0, "pending")
    with pytest.raises(complement.SourceError, match="invalid_year_rows"):
        complement._store(tmp_path, year, [{"date": "2025-12-31", "Price": 1}], now)


def test_status_keeps_unknown_universes_unknown(tmp_path: Path) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        status = complement._status(connection, tmp_path, state="running")
    assert status["series"]["TaiwanStockPrice"]["target"] == 0
    assert status["series"]["GoldPrice"]["target"] == 127
    assert status["news"] == "enabled_whole_market_calendar_day"
    assert "token" not in json.dumps(status)


def test_migrate_denied_marketwide_years_to_free_per_id_without_deleting_receipts(tmp_path: Path) -> None:
    root = tmp_path / "data_finmind" / "complement"
    with complement._db(root / "queue.sqlite3") as connection:
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,error_code) "
            "VALUES ('TaiwanExchangeRate','','2026','year',0,'not_entitled','not_entitled')"
        )
        connection.commit()
        complement._populate(connection, root, today=date(2026, 9, 25))
        assert connection.execute(
            "SELECT state FROM tasks WHERE dataset='TaiwanExchangeRate' AND data_id=''"
        ).fetchone()[0] == "deprecated_query_shape"
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanExchangeRate' AND state='pending'"
        ).fetchone()[0] == len(complement.CURRENCIES)
        status = complement._status(connection, root, state="running")
        assert status["series"]["TaiwanExchangeRate"]["target"] == len(complement.CURRENCIES)
        assert status["series"]["TaiwanExchangeRate"]["not_entitled"] == 0


def test_one_bulk_response_fans_out_and_checks_old_nonempty_year(tmp_path: Path) -> None:
    now = datetime(2002, 9, 25, tzinfo=UTC)
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2002, 9, 25))
        task = complement.Task("TaiwanStockDelisting", "", "2002", "year", 0, "pending")
        assert complement._bulk_needed(connection, task, date(2002, 9, 25))
        complement._store_bulk_years(connection, tmp_path, task, [
            {"date": "2001-01-08", "stock_id": "1234"},
            {"date": "2002-09-20", "stock_id": "5678"},
        ], now, date(2002, 9, 25))
        assert connection.execute(
            "SELECT SUM(rows) FROM tasks WHERE dataset='TaiwanStockDelisting'"
        ).fetchone()[0] == 2
        assert not complement._bulk_needed(connection, task, date(2002, 9, 25))
        with pytest.raises(complement.SourceError, match="incomplete_bulk_response"):
            complement._store_bulk_years(connection, tmp_path, task, [
                {"date": "2002-09-20", "stock_id": "5678"},
            ], now, date(2002, 9, 25))


def test_old_pending_year_does_not_force_refresh_of_not_due_current_year(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        connection.execute(
            "UPDATE tasks SET state='observed_empty', last_attempt_at_utc=?, "
            "next_attempt_at_utc=? WHERE dataset='TaiwanStockParValueChange' AND partition='2026'",
            (now.isoformat(), (now + timedelta(days=1)).isoformat()),
        )
        connection.commit()
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        task = connection.execute(
            "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
            "WHERE dataset='TaiwanStockParValueChange' AND partition='2026'"
        ).fetchone()
        current = complement.Task(*task)
        assert current.state == "observed_empty"
        assert connection.execute(
            "SELECT next_attempt_at_utc FROM tasks WHERE dataset=? AND partition=?",
            (current.dataset, current.partition),
        ).fetchone()[0] > now.isoformat()


def test_wide_institutional_is_derived_without_api_call(tmp_path: Path) -> None:
    now = datetime(2026, 9, 25, tzinfo=UTC)
    long = complement.Task(complement.LONG_INSTITUTIONAL, "2330", "history", "id_history", 2, "pending")
    wide = complement.Task(complement.WIDE_INSTITUTIONAL, "2330", "history", "derived", 3, "pending")
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._add_tasks(connection, [(long.dataset, long.data_id, long.partition, long.kind, long.priority),
                                           (wide.dataset, wide.data_id, wide.partition, wide.kind, wide.priority)])
        receipt = complement._store(tmp_path, long, [
            {"date": "2026-09-24", "stock_id": "2330", "name": "Foreign_Investor", "buy": 3, "sell": 1},
            {"date": "2026-09-24", "stock_id": "2330", "name": "Dealer_self", "buy": 2, "sell": 4},
        ], now)
        complement._save_result(connection, long, receipt, now)
        rows = complement._derive_wide(tmp_path, connection, wide)
        assert len(rows) == 1
        assert rows[0]["Foreign_Investor_buy"] == 3
        assert rows[0]["Dealer_self_sell"] == 4
        assert rows[0]["Dealer_buy"] == 0
        derived = complement._store(tmp_path, wide, rows, now)
        assert derived["derived_from"] == complement.LONG_INSTITUTIONAL
        complement._save_result(connection, wide, derived, now)
        assert connection.execute("SELECT state FROM tasks WHERE dataset=?",
                                  (wide.dataset,)).fetchone()[0] == "complete"


def test_request_uses_free_per_id_and_bulk_history_contract(tmp_path: Path) -> None:
    class Limiter:
        def wait(self) -> None:
            pass

        def defer(self, seconds: int) -> None:
            raise AssertionError(f"unexpected defer {seconds}")

    class Response:
        status_code = 200
        headers: dict[str, str] = {}

        def json(self) -> dict:
            return {"status": 200, "msg": "success", "data": []}

        def iter_content(self, chunk_size):
            yield json.dumps(self.json()).encode()

        def close(self):
            pass

    class Session:
        def __init__(self) -> None:
            self.params: list[dict[str, str]] = []

        def get(self, _url: str, *, params: dict[str, str], **_kwargs: object) -> Response:
            self.params.append(params)
            return Response()

    session = Session()
    fx = complement.Task("TaiwanExchangeRate", "USD", "history", "id_history", 1, "pending")
    whole = complement.Task("TaiwanStockDelisting", "", "2026", "year", 0, "pending")
    complement._request(session, Limiter(), tmp_path, fx, "secret", today=date(2026, 9, 25))
    complement._request(session, Limiter(), tmp_path, whole, "secret",
                        today=date(2026, 9, 25), full_history=True)
    assert session.params == [
        {"dataset": "TaiwanExchangeRate", "data_id": "USD", "start_date": "2006-01-01"},
        {"dataset": "TaiwanStockDelisting", "start_date": "2001-01-01"},
    ]


@pytest.mark.parametrize("http_envelope", [True, False])
@pytest.mark.parametrize("status,message,expected", [
    (400, "Your level is register. Please update your user level", "not_entitled"),
    (400, "Bad parameters", "provider_bad_request"),
    (403, "ip banned", "ip_banned"),
    (403, "Your level is register. Please update your user level", "not_entitled"),
    (403, "Invalid token", "invalid_token"),
    (403, "Too many requests", "rate_limited"),
    (403, "Temporary forbidden", "http_403"),
])
def test_client_errors_are_classified_without_automatic_repeat(
    tmp_path: Path, status: int, message: str, expected: str, http_envelope: bool,
) -> None:
    class Limiter:
        deferred: int | None = None

        def wait(self) -> None:
            pass

        def defer(self, seconds: int) -> None:
            self.deferred = seconds

    class Response:
        status_code = status if http_envelope else 200
        headers: dict[str, str] = {}

        def json(self) -> dict:
            return {"status": status, "msg": message}

        def iter_content(self, chunk_size):
            yield json.dumps(self.json()).encode()

        def close(self):
            pass

    class Session:
        def get(self, *_args: object, **_kwargs: object) -> Response:
            return Response()

    limiter = Limiter()
    task = complement.Task("TaiwanExchangeRate", "USD", "history", "id_history", 1, "pending")
    with pytest.raises(complement.SourceError, match=expected):
        complement._request(Session(), limiter, tmp_path, task, "secret", today=date(2026, 9, 25))
    assert limiter.deferred == {"ip_banned": 1800, "rate_limited": 3600}.get(expected)


def test_explicit_blocked_retry_respects_dataset_scope_and_preserves_payload(tmp_path):
    root = tmp_path / 'complement'
    first = ('TaiwanAssetSwapOptionDaily', '54501', 'history')
    other = ('TaiwanAssetSwapFixedIncomeDaily', '54501', 'history')
    with complement._db(root / 'queue.sqlite3') as connection:
        for key in (first, other):
            connection.execute('INSERT INTO tasks '
                '(dataset,data_id,partition,kind,priority,state,error_code,rows,bytes,receipt_path,last_attempt_at_utc) '
                "VALUES (?,?,?,'id_history',0,'not_entitled','not_entitled',495,12993,'receipts/prior.json',?)",
                (*key, '2026-10-26T16:00:00+00:00'))
        connection.commit()
    assert complement._retry_blocked(root, (first[0],)) == 1
    with complement._db(root / 'queue.sqlite3') as connection:
        one = connection.execute('SELECT state,error_code,rows,bytes,receipt_path,last_attempt_at_utc '
                                 'FROM tasks WHERE dataset=?', (first[0],)).fetchone()
        assert one == ('pending', None, 495, 12993, 'receipts/prior.json', '2026-10-26T16:00:00+00:00')
        assert connection.execute('SELECT state FROM tasks WHERE dataset=?', (other[0],)).fetchone() == ('not_entitled',)
    with pytest.raises(ValueError, match='registered_datasets'):
        complement._retry_blocked(root, ())


def test_unknown_403_keeps_retry_and_does_not_poison_other_dataset_tasks(tmp_path):
    from datetime import timedelta
    root = tmp_path / 'complement'
    now = datetime(2026, 10, 27, 1, tzinfo=UTC)
    task = complement.Task('TaiwanAssetSwapOptionDaily', '54501', 'history', 'id_history', 0, 'pending')
    with complement._db(root / 'queue.sqlite3') as connection:
        for identifier in (task.data_id, '55344'):
            connection.execute('INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) '
                               "VALUES (?,?,'history','id_history',0,'pending')", (task.dataset, identifier))
        connection.commit()
        complement._save_failure(connection, task, complement.SourceError('http_403', retry_after=3600), now)
        assert connection.execute('SELECT state,error_code,next_attempt_at_utc FROM tasks WHERE data_id=?',
                                  (task.data_id,)).fetchone() == ('failed', 'http_403', (now + timedelta(hours=1)).isoformat())
        assert connection.execute("SELECT state FROM tasks WHERE data_id='55344'").fetchone() == ('pending',)


def _minute_publication_rows():
    return [{"date": "2026-10-23 13:31:00", "stock_id": "AAPL",
             "open": 100.123456, "high": 101.23, "low": 99.87, "close": 100.99, "volume": 1789}]


def test_final_parquet_fence_precedes_update_observation_and_receipt(tmp_path, monkeypatch):
    import os
    import stat
    from downloader import artifact_io

    task = complement.Task("USStockPriceMinute", "AAPL", "2026-10-23", "id_day", 0, "pending")
    now = datetime(2026, 10, 24, 15, tzinfo=UTC)
    events = []
    sync, observe = os.fsync, complement.record_success

    def fsync(fd):
        path = Path(os.readlink(f"/proc/self/fd/{fd}"))
        events.append(("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file", path))
        return sync(fd)

    def record(root, selected, rows, receipt, stamp, **kwargs):
        final = root / receipt['parquet_path']
        assert final.is_file() and complement._sha256(final) == receipt['sha256']
        assert ("directory", final.parent) in events
        assert sum(kind == 'file' and path.name == 'latest.parquet' for kind, path in events) == 1
        events.append(('update_observation', final))
        return observe(root, selected, rows, receipt, stamp, **kwargs)

    monkeypatch.setattr(artifact_io.os, 'fsync', fsync)
    monkeypatch.setattr(complement, 'record_success', record)
    receipt = complement._store(tmp_path, task, _minute_publication_rows(), now)
    assert receipt['update_observation']['event'] == 'first_nonempty'
    observation = next(i for i, event in enumerate(events) if event[0] == 'update_observation')
    receipt_syncs = [i for i, (kind, path) in enumerate(events)
                    if kind == 'file' and task.partition + '.json.' in path.name]
    assert receipt_syncs and min(receipt_syncs) > observation


def test_same_parquet_hash_reuses_durable_final_without_syncing_preparation(tmp_path, monkeypatch):
    import os
    from downloader import artifact_io

    task = complement.Task("USStockPriceMinute", "AAPL", "2026-10-23", "id_day", 0, "pending")
    now = datetime(2026, 10, 24, 15, tzinfo=UTC)
    first = complement._store(tmp_path, task, _minute_publication_rows(), now)
    final = tmp_path / first['parquet_path']
    inode, content = final.stat().st_ino, final.read_bytes()
    calls, sync = [], os.fsync

    def fsync(fd):
        calls.append(Path(os.readlink(f"/proc/self/fd/{fd}")))
        return sync(fd)

    monkeypatch.setattr(artifact_io.os, 'fsync', fsync)
    second = complement._store(tmp_path, task, _minute_publication_rows(), now + timedelta(seconds=1))
    assert second['sha256'] == first['sha256']
    assert final.stat().st_ino == inode and final.read_bytes() == content
    assert not any('latest.parquet' in path.name for path in calls)
    assert not (final.parent / 'latest.parquet').exists()
    assert list((tmp_path / 'receipt_history').rglob('*.json'))


def test_failed_final_fence_cannot_publish_observation_or_replace_prior_receipt(tmp_path, monkeypatch):
    task = complement.Task("USStockPriceMinute", "AAPL", "2026-10-23", "id_day", 0, "pending")
    now = datetime(2026, 10, 24, 15, tzinfo=UTC)
    first = complement._store(tmp_path, task, _minute_publication_rows(), now)
    receipt_path, final = tmp_path / first['receipt_path'], tmp_path / first['parquet_path']
    prior = receipt_path.read_bytes(), final.read_bytes()

    def failed_fence(*_a, **_kw):
        raise OSError('injected_final_data_fence_failure')

    def unexpected_observation(*_a, **_kw):
        pytest.fail('source observation published before final data fence')

    monkeypatch.setattr(complement, 'durable_replace', failed_fence)
    monkeypatch.setattr(complement, 'record_success', unexpected_observation)
    changed = [{**row, 'volume': 2001} for row in _minute_publication_rows()]
    with pytest.raises(OSError, match='injected_final_data_fence_failure'):
        complement._store(tmp_path, task, changed, now + timedelta(seconds=1))
    assert (receipt_path.read_bytes(), final.read_bytes()) == prior

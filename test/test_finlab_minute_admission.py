"""Minute-only admission preserves the full inventory's live decision."""
from datetime import UTC, date, datetime, timedelta
import itertools
import json

import pytest

from scripts import download_finlab_market_intraday as market
from scripts import benchmark_finlab_minute_admission as trial
from scripts import verify_finlab_minute_admission_runtime as runtime
from stockagent.data.finlab_quota_policy import QuotaPolicy
from test_finlab_market_intraday import _fixture


DAYS = [date(2026, 10, day) for day in (8, 9, 12, 13, 14)]
STATUSES = ("downloaded_unverified_for_pit", "derived_unverified_for_pit",
            "derived_no_regular_trades", "verified_closed_date", "verified_no_trade",
            "partition_not_ready", "provider_error", "unknown_future_failure")


def item(symbol="2330", *, current=True):
    return {"symbol": symbol, "market": "twse", "security_type": "stock", "current": current,
            "delisting_date": None if current else "2026-10-13",
            "first_candidate_date": "2026-10-08", "first_local_daily_date": "2026-10-08",
            "last_local_daily_date": "2026-10-14"}


def freeze(monkeypatch, now):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)
    monkeypatch.setattr(market, "datetime", FrozenDateTime)


def compare(connection, universe, root, now):
    policy = QuotaPolicy()
    full = market._summarize(connection, universe, DAYS, started=now, attempts=0, successes=0,
                            state="offline_test", source_root=root, quota_policy=policy)
    compact = market._minute_admission(connection, universe, DAYS, source_root=root, quota_policy=policy)
    assert compact == full["minute_priority"]
    return compact, full


@pytest.mark.parametrize("status,source_kind,hour", tuple(itertools.product(
    STATUSES, ("tw_minute", "tw_minute_derived"), (7, 8))))
def test_full_inventory_decision_parity_at_retry_publication_reset_boundaries(
        tmp_path, monkeypatch, status, source_kind, hour):
    now = datetime(2026, 10, 15, hour, 0, tzinfo=market.TAIPEI).astimezone(UTC)
    freeze(monkeypatch, now)
    with market._open_index(tmp_path / "index.sqlite3") as connection:
        market._record(connection, "tw_tick", "2330", DAYS[0], "downloaded_unverified_for_pit", 23, 811)
        for day in DAYS:
            market._record(connection, source_kind, "2330", day, status, 270, 1001)
            _, attempt = market._partition_paths(tmp_path, "tw_minute:2330", day)
            attempt.parent.mkdir(parents=True, exist_ok=True)
            market._atomic_json(attempt, {"status": status, "attempted_at_utc": (now-timedelta(minutes=1)).isoformat(),
                "retry_at_utc": (now+timedelta(seconds=30)).isoformat(), "cycle_attempts": 2})
        compact, _ = compare(connection, [item()], tmp_path, now)
        if status in STATUSES[:5]:
            assert compact["tick_residual_allowed"]
        if status == "provider_error":
            # 08:00 Taipei starts a new cycle: the preceding attempt may retry.
            assert compact["blocked_known_window_partitions"] == (len(DAYS) if hour == 7 else 0)


@pytest.mark.parametrize("minute_present", (False, True))
def test_bounds_keep_known_symbols_horizon_and_minute_preference(tmp_path, monkeypatch, minute_present):
    now = datetime(2026, 10, 15, 4, tzinfo=market.TAIPEI).astimezone(UTC)
    freeze(monkeypatch, now)
    with market._open_index(tmp_path / "index.sqlite3") as connection:
        market._record(connection, "tw_minute", "unknown", DAYS[0], "downloaded_unverified_for_pit")
        market._record(connection, "tw_minute", "2330", date(2000, 1, 1), "downloaded_unverified_for_pit")
        market._record(connection, "tw_tick", "2330", DAYS[1], "downloaded_unverified_for_pit")
        if minute_present:
            market._record(connection, "tw_minute", "2330", DAYS[3], "downloaded_unverified_for_pit")
        compact, full = compare(connection, [item()], tmp_path, now)
        observed = next(phase for phase in full["minute_coverage_phases"] if phase["id"] == "observed_history")
        assert observed["candidate_partitions"] == (1 if minute_present else 3)
        assert compact["pending_known_window_partitions"] == (1 if minute_present else 4)


def test_non_sessions_delisted_and_unknown_failures_remain_ineligible(tmp_path, monkeypatch):
    now = datetime(2026, 10, 15, 4, tzinfo=market.TAIPEI).astimezone(UTC)
    freeze(monkeypatch, now)
    with market._open_index(tmp_path / "index.sqlite3") as connection:
        for day in DAYS:
            market._record(connection, "tw_minute", "2330", day, "verified_no_trade")
        market._record(connection, "tw_tick", "2330", DAYS[0], "downloaded_unverified_for_pit")
        for symbol, day in (("unknown", DAYS[-1]), ("2330", date(2026, 10, 10)), ("former", DAYS[-1])):
            market._record(connection, "tw_minute", symbol, day, "provider_error")
        for day in DAYS[:-1]:
            market._record(connection, "tw_minute", "former", day, "verified_no_trade")
        compact, _ = compare(connection, [item(), item("former", current=False)], tmp_path, now)
        assert compact["pending_known_window_partitions"] == 0
        assert compact["tick_residual_allowed"]


def test_admission_is_live_without_full_inventory_or_tick_coverage(tmp_path, monkeypatch):
    now = datetime(2026, 10, 15, 4, tzinfo=market.TAIPEI).astimezone(UTC)
    freeze(monkeypatch, now)
    with market._open_index(tmp_path / "index.sqlite3") as connection:
        for day in DAYS:
            market._record(connection, "tw_minute", "2330", day, "downloaded_unverified_for_pit")
        expected, _ = compare(connection, [item()], tmp_path, now)
        monkeypatch.setattr(market, "_summarize", lambda *a, **kw: pytest.fail("admission is not a full inventory"))
        phases = market._coverage_phases
        kinds = []
        def observed_phases(*args, **kwargs):
            kinds.append(kwargs["kind"])
            return phases(*args, **kwargs)
        monkeypatch.setattr(market, "_coverage_phases", observed_phases)
        assert market._minute_admission(connection, [item()], DAYS, source_root=tmp_path,
                                        quota_policy=QuotaPolicy()) == expected
        market._record(connection, "tw_minute", "2330", DAYS[-1], "provider_error")
        live = market._minute_admission(connection, [item()], DAYS, source_root=tmp_path,
                                        quota_policy=QuotaPolicy())
        assert live["actionable_known_window_partitions"] == 1
        assert not live["tick_residual_allowed"]
        assert kinds == ["tw_minute", "tw_minute"]


def test_runtime_observation_requires_an_executed_minute_check(tmp_path, monkeypatch):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    root = tmp_path / "finlab"
    monkeypatch.setattr(market, "credential_available", lambda: True)
    monkeypatch.setattr(market, "quota_room_mb", lambda: pytest.fail("minutes precede account quota"))
    market.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24), limit=1,
        reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), kind="tw_tick",
        enforce_minute_priority=True, fetch=lambda *a, **kw: pytest.fail("pending minutes precede tick"))
    progress = json.loads((root / "intraday/active_run.json").read_text())
    observed = progress["minute_priority_admission"]
    assert observed["checks"] == 1
    assert observed["engine"] == "minute_priority_only_v1"
    assert observed["checked_at_utc"]
    assert observed["wall_seconds"] >= 0
    assert not observed["complete_inventory_generated"]
    assert not observed["decision"]["tick_residual_allowed"]


def observation(now):
    return {"observed_at_utc": now.isoformat(), "minute_priority_admission": {
        "contract_version": 1, "engine": "minute_priority_only_v1", "checks": 1,
        "wall_seconds": 0.1, "complete_inventory_generated": False,
        "checked_at_utc": now.isoformat(), "decision": {"tick_residual_allowed": True}}}


@pytest.mark.parametrize("update", ({"checks": 0}, {"checks": True}, {"wall_seconds": float("nan")},
    {"wall_seconds": -1}, {"complete_inventory_generated": True},
    {"checked_at_utc": "2026-10-14T00:00:00+00:00"},
    {"checked_at_utc": "2026-10-16T00:00:00+00:00"},
    {"checked_at_utc": "2026-10-15T00:00:00"}, {"decision": {"tick_residual_allowed": 1}}))
def test_runtime_rejects_labels_without_valid_execution_and_clock(update):
    now = datetime(2026, 10, 15, tzinfo=UTC)
    progress = observation(now)
    progress["minute_priority_admission"].update(update)
    with pytest.raises(ValueError):
        runtime.executed_admission(progress, now=now)


def test_runtime_does_not_promote_old_or_unexecuted_owner_to_adopted():
    now = datetime(2026, 10, 15, tzinfo=UTC)
    assert not runtime.executed_admission({}, now=now)
    assert not runtime.executed_admission({"minute_priority_admission": None}, now=now)
    assert runtime.executed_admission(observation(now), now=now)


def test_benchmark_refuses_source_destination_overlap_before_writes(tmp_path):
    with pytest.raises(ValueError, match="separate"):
        trial.benchmark(tmp_path, tmp_path, tmp_path / "nested", tmp_path / "reference.py")
    assert not (tmp_path / "nested").exists()


def test_benchmark_refuses_overwriting_a_preserved_trial(tmp_path):
    output = tmp_path / "retained"
    output.mkdir()
    with pytest.raises(ValueError, match="fresh"):
        trial.benchmark(tmp_path / "source", tmp_path / "public", output, tmp_path / "reference.py")
    assert output.is_dir()

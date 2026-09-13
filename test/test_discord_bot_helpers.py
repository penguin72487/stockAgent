from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from stockagent.live.report_formatter import INVESTMENT_WARNING

discord = pytest.importorskip("discord")

from services.discord_bot import bot as discord_bot  # noqa: E402


def test_guide_lists_all_tw_execution_modes() -> None:
    guide = discord_bot._guide_message()

    assert "`tw` 舊版 Naive" in guide
    assert "`tw_cash` 現股/T+2" in guide
    assert "`tw_day_trade_multi_basis` Multi-Basis 現股當沖（初始 1,000 萬）" in guide
    assert "`tw_day_trade_100m` 現股當沖（初始 1 億）" in guide
    assert "`tw_day_trade_multi_basis_22` 多基底22 現股當沖" in guide
    assert "`tw_day_trade_multi_basis_projection_l1_gelu`" in guide


def test_both_enabled_multi_basis_day_trades_are_available_in_market_autocomplete(
    monkeypatch,
) -> (
    None
):
    expected = {
        "tw_day_trade_multi_basis",
        "tw_day_trade_multi_basis_22",
        "tw_day_trade_multi_basis_projection_l1_gelu",
    }
    # Autocomplete availability must be deterministic in a clean checkout;
    # ignored deployment artifacts are tested separately by the missing-model
    # filter test below.
    monkeypatch.setattr(
        discord_bot,
        "_market_has_model",
        lambda cfg: cfg.market in expected,
    )
    choices = asyncio.run(discord_bot.market_autocomplete(None, "multi_basis"))

    values = {choice.value for choice in choices}
    assert values == expected


def test_market_autocomplete_omits_enabled_mode_without_checkpoint(monkeypatch) -> None:
    configs = {
        "ready": SimpleNamespace(market="ready", label="Ready"),
        "no_model": SimpleNamespace(market="no_model", label="No Model"),
    }
    monkeypatch.setattr(discord_bot, "_market_configs", lambda: configs)
    monkeypatch.setattr(discord_bot, "_market_enabled", lambda _cfg: True)
    monkeypatch.setattr(
        discord_bot,
        "_market_has_model",
        lambda cfg: cfg.market == "ready",
    )

    choices = asyncio.run(discord_bot.market_autocomplete(None, ""))

    assert [choice.value for choice in choices] == ["ready"]


def test_signal_market_autocomplete_contains_only_scheduled_models(
    monkeypatch,
) -> None:
    configs = {
        "scheduled": SimpleNamespace(market="scheduled", label="Scheduled"),
        "manual": SimpleNamespace(market="manual", label="Manual"),
        "missing": SimpleNamespace(market="missing", label="Missing"),
    }
    monkeypatch.setattr(discord_bot, "_market_configs", lambda: configs)
    monkeypatch.setattr(discord_bot, "_scheduled_markets", lambda: ["scheduled", "missing"])
    monkeypatch.setattr(discord_bot, "_market_enabled", lambda _cfg: True)
    monkeypatch.setattr(
        discord_bot,
        "_market_has_model",
        lambda cfg: cfg.market != "missing",
    )

    choices = asyncio.run(discord_bot.signal_market_autocomplete(None, ""))

    assert [choice.value for choice in choices] == ["scheduled"]


def test_all_four_day_trade_modes_share_the_0900_paper_execution_contract() -> None:
    configs = discord_bot._market_configs()
    markets = (
        "tw_day_trade_multi_basis",
        "tw_day_trade_100m",
        "tw_day_trade_multi_basis_22",
        "tw_day_trade_multi_basis_projection_l1_gelu",
    )

    for market in markets:
        config = configs[market]
        assert config.schedule_time == "09:00"
        assert config.day_trade_simulation_enabled is True
        assert config.day_trade_quote_interval_seconds == 60
        assert config.day_trade_simulation_state_dir == (
            "artifacts/live/tw_day_trade_simulation"
        )
        assert config.completed_session_command == (
            "scripts/finalize_tw_public_completed_session.py",
        )
        assert config.completed_session_timeout_seconds == 600


def test_all_four_day_trade_modes_are_in_the_runtime_schedule() -> None:
    scheduled = set(discord_bot._scheduled_markets())

    assert {
        "tw_day_trade_multi_basis",
        "tw_day_trade_100m",
        "tw_day_trade_multi_basis_22",
        "tw_day_trade_multi_basis_projection_l1_gelu",
    }.issubset(scheduled)


def test_all_four_overnight_adapters_use_1325_latest_quote_schedule(
    monkeypatch,
) -> None:
    configs = discord_bot._market_configs()
    monkeypatch.setattr(discord_bot, "_market_notice", lambda _status: None)
    markets = (
        "tw_overnight_multi_basis",
        "tw_overnight_100m",
        "tw_overnight_multi_basis_22",
        "tw_overnight_multi_basis_projection_l1_gelu",
    )
    assert set(markets).issubset(discord_bot._scheduled_markets())

    for market in markets:
        config = configs[market]
        assert config.schedule_time == "13:25"
        assert config.open_time == "09:00"
        assert config.close_time == "13:30"
        assert config.overnight_simulation_enabled is True
        assert config.day_trade_simulation_enabled is False
        assert config.overnight_simulation_state_dir == (
            "artifacts/live/tw_overnight_simulation"
        )
        kwargs = discord_bot._signal_kwargs(
            market=market,
            prepared_status=SimpleNamespace(market_open=True),
        )
        assert kwargs["day_trade_model_observation"] == "latest_quote"
        assert kwargs["previous_signal_backfill_limit"] == 0
        assert kwargs["ensure_previous_signal"] is False


def test_overnight_scheduler_catches_up_only_before_close(monkeypatch) -> None:
    cfg = discord_bot._market_configs()["tw_overnight_100m"]
    monkeypatch.setattr(
        discord_bot,
        "_scheduled_market_session_day",
        lambda _cfg, _now: (True, "fixture"),
    )
    timezone = ZoneInfo("Asia/Taipei")

    assert discord_bot._scheduled_signal_key(
        cfg, datetime(2026, 9, 9, 13, 25, 0, tzinfo=timezone)
    ) == "2026-09-09:tw_overnight_100m"
    assert discord_bot._scheduled_signal_key(
        cfg, datetime(2026, 9, 9, 13, 29, 59, tzinfo=timezone)
    ) == "2026-09-09:tw_overnight_100m"
    assert discord_bot._scheduled_signal_key(
        cfg, datetime(2026, 9, 9, 13, 30, 0, tzinfo=timezone)
    ) is None


def test_overnight_preclose_prepare_uses_1325_decision_gate(monkeypatch) -> None:
    cfg = discord_bot._market_configs()["tw_overnight_100m"]
    timezone = ZoneInfo("Asia/Taipei")
    monkeypatch.setattr(
        discord_bot,
        "_scheduled_market_session_day",
        lambda _cfg, _now: (True, "fixture"),
    )
    monkeypatch.setattr(
        discord_bot,
        "_preopen_market_ready_for_session",
        lambda _cfg, _day: False,
    )
    monkeypatch.setattr(
        discord_bot,
        "_market_has_generated_signal_for_session",
        lambda _cfg, _day: False,
    )

    assert discord_bot._preopen_prepare_key(
        cfg, datetime(2026, 9, 9, 13, 15, tzinfo=timezone)
    ) == "2026-09-09:tw_overnight_100m:preopen"
    assert discord_bot._preopen_prepare_key(
        cfg, datetime(2026, 9, 9, 13, 24, tzinfo=timezone)
    ) == "2026-09-09:tw_overnight_100m:preopen"
    assert discord_bot._preopen_prepare_key(
        cfg, datetime(2026, 9, 9, 13, 25, tzinfo=timezone)
    ) is None


def test_overnight_preclose_readiness_does_not_require_daytrade_gates(
    monkeypatch, tmp_path: Path
) -> None:
    cfg = discord_bot._market_configs()["tw_overnight_100m"]
    readiness = tmp_path / "preopen_readiness.json"
    readiness.write_text(
        json.dumps(
            {
                "markets": {
                    cfg.market: {
                        "status": "ready",
                        "completed_at": "2026-09-09T13:15:05+08:00",
                        "panel_date": "2026-09-08",
                        "checkpoint_fingerprint": "sha256:test",
                        "symbol_count": 2744,
                        "warm_contract": "overnight_13_25_model_cache",
                        "preopen_price_limits": None,
                        "same_session_eligibility": None,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(discord_bot, "_preopen_readiness_path", lambda: readiness)

    assert discord_bot._preopen_market_ready_for_session(cfg, "2026-09-09")


def test_overnight_preclose_warmup_skips_opening_and_daytrade_proofs(
    monkeypatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_overnight_100m"]
    calls: dict[str, object] = {}
    result = SimpleNamespace(
        summary={
            "panel_date": "2026-09-08",
            "checkpoint_fingerprint": "sha256:test",
            "symbol_count": 2744,
            "live_latency": {},
        }
    )
    monkeypatch.setattr(discord_bot, "_run_pre_signal_command", lambda _cfg: None)
    monkeypatch.setattr(discord_bot, "_clear_runtime_status_cache", lambda: None)
    monkeypatch.setattr(
        discord_bot,
        "_runtime_status",
        lambda _cfg: SimpleNamespace(data=SimpleNamespace(fresh=True)),
    )
    monkeypatch.setattr(
        discord_bot,
        "_warm_or_reuse_tw_mis_opening_receipt",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("MIS must be skipped")),
    )
    monkeypatch.setattr(
        discord_bot,
        "require_exact_session_eligibility",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("day-trade eligibility must be skipped")
        ),
    )

    def fake_signal_kwargs(**kwargs):
        calls["signal_kwargs"] = kwargs
        return {}

    monkeypatch.setattr(discord_bot, "_signal_kwargs", fake_signal_kwargs)
    monkeypatch.setattr(discord_bot, "generate_live_signal", lambda **_kwargs: result)
    monkeypatch.setattr(
        discord_bot,
        "_write_preopen_readiness",
        lambda _cfg, **kwargs: calls.update(readiness=kwargs),
    )

    assert discord_bot._prewarm_market_signal_serialized(cfg) is result
    assert calls["signal_kwargs"]["day_trade_model_observation"] == "session_open"
    assert result.summary["warm_contract"] == "overnight_13_25_model_cache"
    assert calls["readiness"]["status"] == "ready"


def test_startup_warmup_stops_permanent_contract_retry_storm() -> None:
    mismatch = RuntimeError("Checkpoint semantic fingerprint mismatch (model: saved=x)")
    transient = OSError("temporary CUDA device busy")

    assert not discord_bot._startup_warmup_failure_is_retryable(mismatch)
    assert discord_bot._startup_warmup_failure_is_retryable(transient)
    assert discord_bot._startup_warmup_retry_delay_seconds(1) == 60.0
    assert discord_bot._startup_warmup_retry_delay_seconds(2) == 120.0
    assert discord_bot._startup_warmup_retry_delay_seconds(99) == 900.0


def test_day_trade_scheduler_accepts_only_the_true_session_open_artifact() -> None:
    cfg = SimpleNamespace(timezone="Asia/Taipei", open_time="09:00")
    valid = {
        "generated_at": "2026-09-03T09:00:00+08:00",
        "signal_started_at": "2026-09-03T09:00:00.083+08:00",
        "live_session_open_feature_applied": True,
        "day_trade_model_observation": "session_open",
        "signal_price_contract": {
            "model_observation": "session_open",
            "opening_execution_eligible": True,
        },
    }
    preopen = {
        **valid,
        "signal_started_at": "2026-09-03T08:34:23+08:00",
        "live_session_open_feature_applied": False,
        "signal_price_contract": {
            "model_observation": "completed_panel",
            "opening_execution_eligible": False,
        },
    }
    latest_quote = {
        **valid,
        "signal_started_at": "2026-09-03T09:01:43+08:00",
        "live_session_open_feature_applied": False,
        "day_trade_model_observation": "latest_quote",
        "signal_price_contract": {
            "model_observation": "intraday_latest_quote",
            "opening_execution_eligible": False,
        },
    }

    assert discord_bot._is_scheduled_day_trade_opening_signal(
        cfg, valid, "2026-09-03"
    )
    assert not discord_bot._is_scheduled_day_trade_opening_signal(
        cfg, preopen, "2026-09-03"
    )
    assert not discord_bot._is_scheduled_day_trade_opening_signal(
        cfg, latest_quote, "2026-09-03"
    )


def test_day_trade_model_uses_shared_official_opening_snapshot() -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]

    source = discord_bot._auto_signal_price_source(
        cfg,
        SimpleNamespace(market_open=True),
        "auto",
    )

    assert source == "tw"


def test_opening_signal_latency_record_preserves_stage_and_source_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = SimpleNamespace(
        market="mode-a",
        timezone="Asia/Taipei",
        open_time="09:00",
    )
    result = SimpleNamespace(
        summary={
            "signal_id": "signal-a",
            "signal_started_at": "2026-09-09T09:00:00.050000+08:00",
            "signal_ready_at": "2026-09-09T09:00:00.800000+08:00",
            "artifact_published_at": "2026-09-09T09:00:00.825000+08:00",
            "price_source": "shioaji:fixture",
            "price_receipt_timing": {
                "coverage_receipt_from_open_ms": 300.0,
                "coverage_to_signal_ready_ms": 500.0,
            },
            "live_latency": {
                "quote_fetch_ms": 250.0,
                "model_inference_ms": 40.0,
                "artifact_publish_ms": 25.0,
                "quote_transport": {"server_provider_fetch_ms": 200.0},
            },
        }
    )
    timing = {
        "scheduler_observed_at": "2026-09-09T09:00:00.020000+08:00",
        "attempt_started_at": "2026-09-09T09:00:00.025000+08:00",
        "scheduler_wake_ms": 20.0,
        "realtime_prepare_ms": 15.0,
        "model_lock_queue_ms": 5.0,
        "ensure_previous_signal": False,
        "previous_signal_backfill_limit": 0,
    }

    payload = discord_bot._opening_signal_latency_record(
        cfg=cfg,
        session_date="2026-09-09",
        schedule_key="2026-09-09:mode-a",
        timing=timing,
        result=result,
        error=None,
    )

    assert payload["status"] == "ready"
    assert payload["goal_ms"] == 1_000.0
    assert payload["ready_from_open_ms"] == 800.0
    assert payload["source_ready_from_open_ms"] == 300.0
    assert payload["source_ready_to_signal_ms"] == 500.0
    assert payload["stages"]["model_inference_ms"] == 40.0
    assert payload["previous_signal_history_disabled"] is True

    path = tmp_path / "opening_signal_latency.jsonl"
    monkeypatch.setattr(discord_bot, "_opening_signal_latency_path", lambda: path)
    discord_bot._record_opening_signal_latency(payload)
    assert json.loads(path.read_text(encoding="utf-8"))["signal_id"] == "signal-a"


def test_day_trade_signal_kwargs_never_recomputes_irrelevant_previous_holdings(
    monkeypatch,
) -> None:
    cfg = discord_bot._market_configs()[
        "tw_day_trade_multi_basis_projection_l1_gelu"
    ]
    monkeypatch.setattr(discord_bot, "_effective_market_config", lambda value: value)
    monkeypatch.setattr(discord_bot, "_resolve_market", lambda _market: cfg)
    monkeypatch.setattr(discord_bot, "_ensure_signal_ready", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(discord_bot, "_market_notice", lambda _status: "")
    monkeypatch.setenv("STOCKAGENT_SIGNAL_BACKFILL_LIMIT", "32")

    kwargs = discord_bot._signal_kwargs(market=cfg.market)

    assert kwargs["ensure_previous_signal"] is False
    assert kwargs["previous_signal_backfill_limit"] == 0


def test_day_trade_failure_retry_ignores_slow_batch_retry_setting(monkeypatch) -> None:
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_RETRY_DELAY_SECONDS", "900")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_RETRY_BASE_SECONDS", "0.25")
    retry_after: dict[str, float] = {}
    failures: dict[str, int] = {}

    first = discord_bot._mark_signal_retry(
        retry_after,
        failures,
        "session:market",
        day_trade=True,
    )
    second = discord_bot._mark_signal_retry(
        retry_after,
        failures,
        "session:market",
        day_trade=True,
    )

    assert first == 0.25
    assert second == 0.5


def test_artifact_backfill_reconciles_completed_external_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_multi_basis_projection_l1_gelu"]
    finished: list[tuple[str, str, str]] = []
    synced: list[str] = []
    key = "2026-08-26:tw_day_trade_multi_basis_projection_l1_gelu:artifact_backfill"
    discord_bot.bot._last_artifact_backfill_keys.discard(key)
    monkeypatch.setattr(discord_bot, "_effective_market_config", lambda value: value)
    monkeypatch.setattr(discord_bot, "_ensure_signal_ready", lambda _cfg: object())
    monkeypatch.setattr(discord_bot, "_market_execution_mode", lambda _cfg: "tw_day_trade")
    monkeypatch.setattr(
        discord_bot,
        "_artifact_backfill_is_current",
        lambda _cfg, _status, _mode: True,
    )
    monkeypatch.setattr(
        discord_bot,
        "_sync_latest_live_weights_to_market_artifact",
        lambda _cfg: synced.append(_cfg.market),
    )
    monkeypatch.setattr(
        discord_bot,
        "_finish_artifact_backfill",
        lambda job_key, market, *, status: finished.append(
            (job_key, market, status)
        ),
    )

    assert discord_bot._reconcile_artifact_backfill_if_current(
        cfg,
        key=key,
        market=cfg.market,
    )
    assert synced == [cfg.market]
    assert finished == [(key, cfg.market, "ready")]
    assert key in discord_bot.bot._last_artifact_backfill_keys
    discord_bot.bot._last_artifact_backfill_keys.discard(key)


def test_artifact_backfill_keeps_runtime_scan_off_event_loop() -> None:
    source = inspect.getsource(discord_bot.artifact_backfill.coro)

    assert "await asyncio.to_thread(_artifact_backfill_key, cfg, now)" in source


def test_post_open_catch_up_stops_once_preopen_contract_is_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]
    observed = datetime(2026, 8, 25, 9, 30, tzinfo=ZoneInfo("Asia/Taipei"))

    monkeypatch.setattr(
        discord_bot,
        "_preopen_market_ready_for_session",
        lambda _cfg, _session: True,
    )
    assert discord_bot._scheduled_signal_requires_preopen_catch_up(cfg, observed) is False

    monkeypatch.setattr(
        discord_bot,
        "_preopen_market_ready_for_session",
        lambda _cfg, _session: False,
    )
    assert discord_bot._scheduled_signal_requires_preopen_catch_up(cfg, observed) is True


def test_mis_warm_failure_reuses_only_ready_same_session_receipt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        discord_bot,
        "warm_tw_mis_quote_client",
        lambda **_kwargs: (_ for _ in ()).throw(
            ConnectionError("MIS unavailable")
        ),
    )
    monkeypatch.setattr(
        discord_bot,
        "tw_mis_opening_receipt_status",
        lambda **_kwargs: {
            "ready": True,
            "row_count": 12,
            "path": str(tmp_path / "2026-08-25.json"),
        },
    )

    result = discord_bot._warm_or_reuse_tw_mis_opening_receipt(
        parquet_root=tmp_path,
        session_date="2026-08-25",
    )

    assert result["ready"] is True
    assert result["row_count"] == 12
    assert result["source"] == "twse_tpex:mis"
    assert result["proof"] == "receipt_backed_same_session_opening"


def test_mis_warm_failure_without_receipt_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        discord_bot,
        "warm_tw_mis_quote_client",
        lambda **_kwargs: (_ for _ in ()).throw(
            ConnectionError("MIS unavailable")
        ),
    )
    monkeypatch.setattr(
        discord_bot,
        "tw_mis_opening_receipt_status",
        lambda **_kwargs: {"ready": False, "row_count": 0, "path": "missing"},
    )

    with pytest.raises(ConnectionError, match="MIS unavailable"):
        discord_bot._warm_or_reuse_tw_mis_opening_receipt(
            parquet_root=tmp_path,
            session_date="2026-08-25",
        )


def test_post_open_preparation_stops_after_engine_accepts_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]
    observed = datetime(2026, 8, 25, 9, 30, tzinfo=ZoneInfo("Asia/Taipei"))
    monkeypatch.setattr(
        discord_bot,
        "_scheduled_market_session_day",
        lambda _cfg, _now: (True, "calendar session"),
    )
    monkeypatch.setattr(
        discord_bot,
        "_day_trade_schedule_state",
        lambda _cfg, _session: "blocked_open_position",
    )

    assert discord_bot._preopen_prepare_key(cfg, observed) is None


def test_recent_day_trade_artifact_waits_for_engine_instead_of_recomputing(
    monkeypatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]
    # Keep the fixture after the 09:00 execution gate.  Using wall-clock
    # ``now`` made this contract test fail whenever the suite ran pre-open.
    observed = datetime(2026, 9, 4, 10, 0, tzinfo=ZoneInfo("Asia/Taipei"))
    summary = {
        "generated_at": observed.isoformat(),
        "signal_started_at": observed.isoformat(),
        "artifact_published_at": observed.isoformat(),
        "live_session_open_feature_applied": True,
        "day_trade_model_observation": "session_open",
        "signal_price_contract": {
            "model_observation": "session_open",
            "opening_execution_eligible": True,
        },
    }
    monkeypatch.setattr(
        discord_bot,
        "_latest_market_signal",
        lambda _cfg: (SimpleNamespace(), summary),
    )
    monkeypatch.setattr(
        discord_bot,
        "load_service_sync",
        lambda _path: {
            "modes": {
                cfg.market: {
                    "session_date": observed.date().isoformat(),
                    "entry_completed_at": None,
                    "open_position_count": 0,
                }
            }
        },
    )

    assert (
        discord_bot._day_trade_schedule_state(cfg, observed.date().isoformat())
        == "pending_confirmation"
    )
    summary["artifact_published_at"] = (observed - timedelta(hours=3)).isoformat()
    assert (
        discord_bot._day_trade_schedule_state(cfg, observed.date().isoformat())
        == "pending_confirmation"
    )


@pytest.mark.parametrize("authorized", [False, True])
def test_prior_margin_carry_does_not_suppress_next_opening(monkeypatch, authorized):
    from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT
    cfg = SimpleNamespace(market="new", timezone="Asia/Taipei", open_time="09:00",
                          day_trade_residual_margin_conversion=authorized)
    mode = {"session_date": "2026-09-09", "entry_completed_at": "2026-09-09T09:01:00+08:00",
            "open_position_count": 1, "margin_carry_position_count": 1,
            "margin_carry_contract": MARGIN_CARRY_CONTRACT,
            "closing_auction_settled_at": "2026-09-09T13:30:00+08:00",
            "residual_conversion_completed_at": "2026-09-09T13:30:00+08:00"}
    summary = {"generated_at": "2026-09-10T10:00:00+08:00",
               "replay_effective_signal_at": "2026-09-09T09:00:00+08:00",
               "live_session_open_feature_applied": True,
               "day_trade_model_observation": "session_open",
               "signal_price_contract": {"model_observation": "session_open", "opening_execution_eligible": True}}
    monkeypatch.setattr(discord_bot, "load_service_sync", lambda _: {"modes": {"new": mode}})
    monkeypatch.setattr(discord_bot, "_latest_market_signal", lambda _: (None, summary))
    assert discord_bot._day_trade_schedule_state(cfg, "2026-09-10") == ("retry" if authorized else "blocked_open_position")
    assert not discord_bot._is_scheduled_day_trade_opening_signal(cfg, summary, "2026-09-10")
    if authorized:
        summary["replay_effective_signal_at"] = "2026-09-10T09:00:00+08:00"
        assert discord_bot._day_trade_schedule_state(cfg, "2026-09-10") == "pending_confirmation"
        mode.pop("residual_conversion_completed_at")
        assert discord_bot._day_trade_schedule_state(cfg, "2026-09-10") == "blocked_open_position"


def test_committed_current_session_is_not_recomputed_when_pointer_is_missing(monkeypatch):
    cfg = SimpleNamespace(market="new")
    monkeypatch.setattr(discord_bot, "load_service_sync", lambda _: {"modes": {"new": {
        "session_date": "2026-09-10", "entry_completed_at": "2026-09-10T09:01:00+08:00",
        "open_position_count": 1}}})
    monkeypatch.setattr(discord_bot, "_latest_market_signal", lambda _: None)
    assert discord_bot._day_trade_schedule_state(cfg, "2026-09-10") == "completed"


def test_discord_page_size_and_top_n_floor_to_ten() -> None:
    assert discord_bot._page_size(1) == 10
    assert discord_bot._page_size(5) == 10
    assert discord_bot._page_size(None) == 20
    assert discord_bot._page_size(99) == 40

    assert discord_bot._top_n(1) == 10
    assert discord_bot._top_n(None) == 20


def test_discord_line_pages_can_opt_into_one_row_per_page() -> None:
    rows = [{"symbol": f"S{i:02d}"} for i in range(3)]
    pages = discord_bot._line_pages(
        title="one row",
        rows=rows,
        formatter=lambda row: str(row["symbol"]),
        page_size=1,
        min_page_size=1,
        default_page_size=1,
    )

    assert len(pages) == 3
    assert "`rows 1-1/3`" in pages[0]
    assert "`rows 2-2/3`" in pages[1]
    assert "`rows 3-3/3`" in pages[2]
    assert "S00" in pages[0]
    assert "S01" not in pages[0]


def test_discord_line_pages_use_minimum_ten_rows_and_warning() -> None:
    rows = [{"symbol": f"S{i:02d}"} for i in range(12)]
    pages = discord_bot._line_pages(
        title="test rows",
        rows=rows,
        formatter=lambda row: str(row["symbol"]),
        page_size=5,
    )

    assert len(pages) == 2
    assert "`rows 1-10/12`" in pages[0]
    assert "S09" in pages[0]
    assert "`rows 11-12/12`" in pages[1]
    assert all(INVESTMENT_WARNING in page for page in pages)


def test_discord_empty_trade_page_still_has_warning() -> None:
    pages = discord_bot._line_pages(title="empty", rows=[], formatter=str, page_size=5)

    assert pages == [f"**empty**\n(no rows)\n\n{INVESTMENT_WARNING}"]


def test_user_facing_commands_support_user_install_and_private_contexts() -> None:
    shared_state_commands = {"set_market_enabled", "set_schedule", "set_capital"}

    for command in discord_bot.bot.tree.get_commands():
        payload = command.to_dict(discord_bot.bot.tree)
        if command.name in shared_state_commands:
            assert payload["integration_types"] == [0]
            assert payload["contexts"] == [0]
        else:
            assert payload["integration_types"] == [0, 1]
            assert payload["contexts"] == [0, 1, 2]

    ask_command = discord_bot.bot.tree.get_command("ask")
    assert ask_command is not None
    assert [parameter.name for parameter in ask_command.parameters] == ["question"]


def test_setup_hook_syncs_only_global_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    sync_guilds: list[object | None] = []
    started_loops: list[str] = []
    startup_events: list[str] = []

    async def fake_sync(_tree, *, guild=None):
        startup_events.append("sync")
        sync_guilds.append(guild)
        return []

    def fake_start(loop, *args, **kwargs):
        del args, kwargs
        started_loops.append(loop.coro.__name__)
        startup_events.append(f"start:{loop.coro.__name__}")

    monkeypatch.setattr(discord_bot.app_commands.CommandTree, "sync", fake_sync)
    monkeypatch.setattr(discord_bot.tasks.Loop, "start", fake_start)

    asyncio.run(discord_bot.bot.setup_hook())

    assert sync_guilds == [None]
    assert set(started_loops) == {
        "scheduled_signal",
        "postclose_fast_arm",
        "postclose_fast_cache",
        "startup_inference_warmup",
        "service_heartbeat",
        "signal_now_job_resumer",
        "preopen_prepare",
        "daily_summary",
        "model_auto_deployment",
    }
    assert "artifact_backfill" not in started_loops
    assert startup_events[:5] == [
        "start:postclose_fast_arm",
        "start:postclose_fast_cache",
        "start:scheduled_signal",
        "start:service_heartbeat",
        "sync",
    ]


def test_postclose_artifact_maintenance_isolated_from_discord_cgroup() -> None:
    service = Path(
        "deploy/systemd/stockagent-discord-artifact-maintenance.service.in"
    ).read_text(encoding="utf-8")
    timer = Path(
        "deploy/systemd/stockagent-discord-artifact-maintenance.timer.in"
    ).read_text(encoding="utf-8")
    path_unit = Path(
        "deploy/systemd/stockagent-discord-artifact-maintenance.path.in"
    ).read_text(encoding="utf-8")
    cache_service = Path(
        "deploy/systemd/stockagent-discord-postclose-cache.service.in"
    ).read_text(encoding="utf-8")
    installer = Path("scripts/install_discord_bot_service.sh").read_text(
        encoding="utf-8"
    )

    assert "Type=oneshot" in service
    assert "CPUWeight=10" in service
    assert "IOWeight=10" in service
    assert "OOMScoreAdjust=500" in service
    assert "run_discord_artifact_maintenance.sh" in service
    assert "OnCalendar=Mon..Fri" in timer
    assert "RandomizedDelaySec=0" in timer
    assert "stockagent-discord-artifact-maintenance.timer" in installer
    assert "completed_session/latest.json" in path_unit
    assert "stockagent-discord-postclose-cache.service" in path_unit
    assert "--signal-cache-only" in cache_service
    assert "TimeoutStartSec=10m" in cache_service
    assert "stockagent-discord-artifact-maintenance.path" in installer
    assert "stockagent-discord-postclose-cache.service" in installer

    gateway = Path(
        "deploy/systemd/stockagent-discord-bot.service.in"
    ).read_text(encoding="utf-8")
    assert "OOMScoreAdjust=-500" in gateway
    assert "Nice=-5" in gateway
    assert "CPUWeight=200" in gateway
    assert "IOWeight=200" in gateway


def test_event_driven_postclose_service_never_runs_formal_history(
    monkeypatch,
    tmp_path,
) -> None:
    from scripts import run_discord_artifact_maintenance as runner

    calls: list[object] = []
    monkeypatch.setattr(runner.discord_bot, "_rotate_error_log_if_needed", lambda: None)
    monkeypatch.setattr(
        runner.discord_bot,
        "_artifact_backfill_status_path",
        lambda: tmp_path / "artifact_status.json",
    )
    monkeypatch.setattr(runner.discord_bot, "_opening_critical_work_pending", lambda: False)
    monkeypatch.setattr(runner.discord_bot, "_interactive_signal_work_pending", lambda: False)
    monkeypatch.setattr(runner.discord_bot, "_tw_public_refresh_in_progress", lambda: False)
    monkeypatch.setattr(
        runner.discord_bot,
        "_artifact_maintenance_markets",
        lambda: ["tw_day_trade_multi_basis"],
    )
    monkeypatch.setattr(
        runner,
        "_populate_postclose_signal_caches",
        lambda markets: calls.append(tuple(markets)) or (1, 0),
    )
    monkeypatch.setattr(
        runner.discord_bot,
        "_artifact_backfill_key",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("formal history must not run")
        ),
    )

    assert runner.run_once(signal_cache_only=True) == 0
    assert calls == [("tw_day_trade_multi_basis",)]


def test_opening_watchdog_cannot_undercut_bounded_quote_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOCKAGENT_OPENING_HOT_ATTEMPT_TIMEOUT_SECONDS", "5")
    monkeypatch.setenv("STOCKAGENT_OPENING_COLD_ATTEMPT_TIMEOUT_SECONDS", "5")

    assert discord_bot._opening_attempt_timeout_seconds(hot=True) == 60.0
    assert discord_bot._opening_attempt_timeout_seconds(hot=False) == 180.0

    monkeypatch.setenv("STOCKAGENT_OPENING_HOT_ATTEMPT_TIMEOUT_SECONDS", "90")
    assert discord_bot._opening_attempt_timeout_seconds(hot=True) == 90.0


def test_final_arm_is_process_scoped_and_requires_opening_source_prewarm(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]
    path = tmp_path / "preopen_readiness.json"
    monkeypatch.setattr(discord_bot, "_preopen_readiness_path", lambda: path)
    payload = {
        "markets": {
            cfg.market: {
                "final_arm": {
                    "status": "ready",
                    "run_id": discord_bot._BOT_RUN_ID,
                    "completed_at": "2026-08-27T08:55:00+08:00",
                    "live_latency": {
                        "panel_cache_hit": True,
                        "checkpoint_cache_hit": True,
                        "model_cache_hit": True,
                    },
                    "opening_source_prewarm": {
                        "ready": True,
                        "run_id": discord_bot._BOT_RUN_ID,
                        "source": "twse_tpex:mis",
                    },
                }
            }
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert discord_bot._preopen_market_final_armed_for_session(
        cfg, "2026-08-27"
    )

    payload["markets"][cfg.market]["final_arm"]["run_id"] = "previous-process"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert not discord_bot._preopen_market_final_armed_for_session(
        cfg, "2026-08-27"
    )

    payload["markets"][cfg.market]["final_arm"]["run_id"] = (
        discord_bot._BOT_RUN_ID
    )
    payload["markets"][cfg.market]["final_arm"].pop(
        "opening_source_prewarm"
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert not discord_bot._preopen_market_final_armed_for_session(
        cfg, "2026-08-27"
    )


def test_day_trade_opening_quote_symbols_keep_only_alive_rows() -> None:
    assert discord_bot._day_trade_opening_quote_symbols(
        [
            {"symbol": "2330", "alive": True},
            {"symbol": "2317", "alive": False},
            {"symbol": "2330", "alive": True},
        ]
    ) == ["2330"]


def test_day_trade_opening_fallback_prices_align_to_alive_symbols() -> None:
    rows = [
        {"symbol": "2330", "alive": True, "current_price": 100.0},
        {"symbol": "1101", "alive": False, "current_price": 40.0},
        {"symbol": "2454", "alive": True, "current_price": 800.0},
    ]
    symbols = discord_bot._day_trade_opening_quote_symbols(rows)

    fallback = discord_bot._day_trade_opening_fallback_prices(rows, symbols)

    assert symbols == ["2330", "2454"]
    assert fallback.tolist() == [100.0, 800.0]

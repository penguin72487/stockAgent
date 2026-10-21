"""A larger existing owner repairs a cold seed, not a stale GPU inference."""
import fcntl
import json
from types import SimpleNamespace

import pytest

from scripts import run_discord_artifact_maintenance as runner
from stockagent.data.annual_cache_admission import CONTRACT
from stockagent.live.market_config import LiveMarketConfig


@pytest.fixture
def owner(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    cfg = LiveMarketConfig(market="factorized", label="factorized", config_path="",
        completed_session_timeout_seconds=1800, completed_session_command=(
        str(tmp_path / "scripts/update_tw_day_trade_factorized_live.py"), "--phase", "completed"))
    path = tmp_path / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/updater_status.json"
    path.parent.mkdir(parents=True)
    state = {"status": "cache_warming_required", "error_type": "AnnualCacheBuildDeferred",
             "last_good_generation_retained": True, "admission": {"contract": CONTRACT}}
    path.write_text(json.dumps(state))
    calls, emissions = [], []
    status = SimpleNamespace(market_open=False, data=SimpleNamespace(fresh=False))
    def command(got, *, bypass_cache):
        assert got is not cfg and bypass_cache is True
        assert got.market == cfg.market and got.completed_session_command == cfg.completed_session_command
        assert got.completed_session_timeout_seconds == runner.FACTORIZED_SOURCE_RECOVERY_SECONDS == 4800
        assert cfg.completed_session_timeout_seconds == 1800
        calls.append("canonical_source")
        path.write_text(json.dumps({"status": "verified"}))
    bot = SimpleNamespace(_ensure_signal_ready=lambda _: status,
                          _completed_session_publication_ready=lambda _: True,
                          _run_completed_session_command=command)
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: bot)
    monkeypatch.setattr(runner, "_emit", lambda **event: emissions.append(event))
    return cfg, path, state, status, bot, calls, emissions


def test_the_existing_owner_can_repair_before_the_derived_panel_is_fresh(owner):
    cfg, _path, _state, _status, _bot, calls, emissions = owner
    assert runner._recover_deferred_factorized_source(cfg)
    assert calls == ["canonical_source"]
    assert [e["status"] for e in emissions] == ["factorized_cache_warming_started",
                                               "factorized_cache_warming_verified"]
    assert all(event["model_executed"] is False for event in emissions)
    assert runner._recover_deferred_factorized_source(cfg)
    assert calls == ["canonical_source"]  # No repeated source command after acceptance.


@pytest.mark.parametrize("field,value", [("phase", "completed"), ("error_type", "unknown"),
    ("model_executed", True), ("last_good_generation_retained", False)])
def test_unverified_preopen_hint_cannot_trigger_background_preparation(owner, field, value):
    cfg, path, _state, _status, _bot, calls, _emissions = owner
    state = {"status": "projection_refresh_required", "phase": "preopen",
        "error_type": "FactorizedPreopenProjectionPending", "model_executed": False,
        "last_good_generation_retained": True}
    state[field] = value
    path.write_text(json.dumps(state))
    with pytest.raises(ValueError, match="unverified factorized"):
        runner._recover_deferred_factorized_source(cfg)
    assert calls == []


@pytest.mark.parametrize("blocked", ["open", "publication"])
def test_recovery_cannot_bypass_live_session_or_official_publication(owner, blocked):
    cfg, _path, _state, status, bot, calls, emissions = owner
    if blocked == "open":
        status.market_open = True
    else:
        bot._completed_session_publication_ready = lambda _: False
    assert runner._recover_deferred_factorized_source(cfg) is False
    assert calls == [] and emissions[-1]["model_executed"] is False


@pytest.mark.parametrize("state_name", ["verified", "already_current", "failed_retryable", "waiting_source"])
def test_ordinary_cache_pass_does_not_invoke_any_source_command(owner, state_name):
    cfg, path, _state, _status, _bot, calls, emissions = owner
    path.write_text(json.dumps({"status": state_name}))
    assert runner._recover_deferred_factorized_source(cfg)
    assert calls == emissions == []


def test_another_market_updater_cannot_consume_the_request(owner):
    cfg, _path, _state, _status, _bot, calls, _emissions = owner
    cfg.completed_session_command = ("scripts/another_source.py",)
    assert runner._recover_deferred_factorized_source(cfg)
    assert calls == []


@pytest.mark.parametrize("change", ["contract", "error", "retained"])
def test_unverified_request_never_invokes_the_configured_source(owner, change):
    cfg, path, state, _status, _bot, calls, _emissions = owner
    if change == "contract":
        state["admission"]["contract"] = "unknown"
    elif change == "error":
        state["error_type"] = "SomeOtherFailure"
    else:
        state["last_good_generation_retained"] = False
    path.write_text(json.dumps(state))
    with pytest.raises(ValueError, match="unverified factorized"):
        runner._recover_deferred_factorized_source(cfg)
    assert calls == []


def test_transport_success_is_not_accepted_source_recovery(owner):
    cfg, _path, _state, _status, bot, calls, _emissions = owner
    bot._run_completed_session_command = lambda *_a, **_kw: calls.append("accepted_no_receipt")
    with pytest.raises(RuntimeError, match="no accepted receipt"):
        runner._recover_deferred_factorized_source(cfg)


def test_lightweight_close_bridge_defers_cold_warming_before_loading_any_owner(owner, monkeypatch):
    cfg, _path, _state, _status, _bot, calls, emissions = owner
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: pytest.fail("bridge must not load a source owner"))
    assert runner._recover_deferred_factorized_source(cfg, allow_warming=False) is False
    assert calls == []
    assert emissions[-1]["reason"] == "isolated_source_owner_required"
    assert emissions[-1]["owner_unit"] == runner.ARTIFACT_OWNER_UNIT
    assert emissions[-1]["model_executed"] is False


def test_canonical_relative_command_is_bound_to_the_repository_not_shell_cwd(owner):
    cfg, _path, _state, _status, _bot, calls, _emissions = owner
    cfg.completed_session_command = ("scripts/update_tw_day_trade_factorized_live.py", "--phase", "completed")
    assert runner._recover_deferred_factorized_source(cfg)
    assert calls == ["canonical_source"]


def test_deferred_cold_market_does_not_block_other_close_cache_consumers(owner, monkeypatch):
    cfg, _path, _state, _status, bot, calls, emissions = owner
    other = SimpleNamespace(market="other", completed_session_command=("scripts/another_updater.py",))
    cached = []
    bot._resolve_market = lambda name: cfg if name == cfg.market else other
    bot._is_tw_daily_signal_market = lambda _cfg: True
    bot._run_completed_session_signal_cache_sync = lambda current: cached.append(current.market)
    bot._log_exception = lambda *_a: pytest.fail("normal deferral is not an exception")
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: bot)
    assert runner._populate_postclose_signal_caches([cfg.market, other.market], allow_source_warming=False) == (2, 0, 1)
    assert calls == [] and cached == [other.market]
    assert emissions[-1]["reason"] == "isolated_source_owner_required"


@pytest.mark.parametrize("lease", [None, fcntl.LOCK_SH, fcntl.LOCK_EX])
def test_factorized_busy_probe_requires_a_live_exclusive_kernel_lease(owner, lease):
    cfg, path, _state, _status, _bot, _calls, _emissions = owner
    lock = path.with_name("prepare.lock")
    assert not runner._factorized_source_preparation_busy(cfg)
    with lock.open("a+") as handle:
        if lease is not None:
            fcntl.flock(handle.fileno(), lease)
        assert runner._factorized_source_preparation_busy(cfg) is (lease == fcntl.LOCK_EX)
        unrelated = SimpleNamespace(completed_session_command=("scripts/another_updater.py",))
        assert not runner._factorized_source_preparation_busy(unrelated)
    assert not runner._factorized_source_preparation_busy(cfg)


def test_active_producer_defers_only_its_market_without_another_child_or_attempt(owner, monkeypatch):
    cfg, path, _state, _status, bot, calls, emissions = owner
    other = SimpleNamespace(market="other", completed_session_command=("scripts/another_updater.py",))
    cached = []
    bot._resolve_market = lambda name: cfg if name == cfg.market else other
    bot._is_tw_daily_signal_market = lambda _cfg: True
    bot._run_completed_session_signal_cache_sync = lambda current: cached.append(current.market)
    bot._log_exception = lambda *_a: pytest.fail("normal producer wait is not an exception")
    before = path.read_bytes()
    with path.with_name("prepare.lock").open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        assert runner._populate_postclose_signal_caches([cfg.market, other.market]) == (2, 0, 1)
    assert calls == [] and cached == [other.market]
    assert path.read_bytes() == before
    assert emissions[-1]["reason"] == "factorized_projection_in_progress"
    assert emissions[-1]["model_executed"] is False


def test_changed_loaded_code_stops_the_process_not_other_market_retry_budgets(owner, monkeypatch):
    from stockagent.live.completed_inference_cache import LoadedImplementationChanged

    cfg, path, _state, _status, bot, calls, _emissions = owner
    path.write_text(json.dumps({"status": "verified"}))
    bot._resolve_market = lambda _: cfg
    bot._is_tw_daily_signal_market = lambda _cfg: True
    def changed(_cfg):
        calls.append("cache")
        raise LoadedImplementationChanged()
    bot._run_completed_session_signal_cache_sync = changed
    bot._log_exception = lambda *_a: pytest.fail("global drift is not a per-market cache error")
    with pytest.raises(LoadedImplementationChanged):
        runner._populate_postclose_signal_caches([cfg.market, "next"])
    assert calls == ["cache"]

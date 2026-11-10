from datetime import datetime
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts import run_discord_artifact_maintenance as runner


@pytest.fixture
def year_policy(tmp_path, monkeypatch):
    path = tmp_path / "scope.json"
    monkeypatch.setenv("STOCKAGENT_FORMAL_HISTORY_SCOPE_POLICY", str(path))
    policy = {"contract": runner.CURRENT_YEAR_HISTORY_CONTRACT,
              "enabled": True, "calendar_year": "current"}
    path.write_text(json.dumps(policy))
    return path, policy


def test_scope_resolves_current_taipei_year_without_a_fixed_year(year_policy):
    assert runner._current_history_year(now=datetime(2026, 11, 10, tzinfo=ZoneInfo("Asia/Taipei"))) == 2026
    assert runner._current_history_year(now=datetime(2027, 1, 1, tzinfo=ZoneInfo("Asia/Taipei"))) == 2027


@pytest.mark.parametrize("year", [True, 2025, 2027, "2026", None])
def test_noncurrent_or_noncanonical_year_is_rejected(year_policy, year):
    path, policy = year_policy
    policy["calendar_year"] = year
    path.write_text(json.dumps(policy))
    with pytest.raises(ValueError, match="current calendar year"):
        runner._current_history_year(now=datetime(2026, 11, 10, tzinfo=ZoneInfo("Asia/Taipei")))


def test_legacy_full_worker_is_deferred_but_compatible_owner_is_admitted(year_policy):
    bot = SimpleNamespace(_resolve_market=lambda _: SimpleNamespace(day_trade_simulation_enabled=True))
    assert runner._year_scope_requires_new_owner(["tw"], bot) is not None
    bot.FORMAL_HISTORY_SCOPE_CONTRACT = runner.CURRENT_YEAR_HISTORY_CONTRACT
    assert runner._year_scope_requires_new_owner(["tw"], bot) is None


def test_non_tw_worker_and_explicit_disabled_policy_preserve_original_behavior(year_policy):
    path, policy = year_policy
    bot = SimpleNamespace(_resolve_market=lambda _: SimpleNamespace(day_trade_simulation_enabled=False))
    assert runner._year_scope_requires_new_owner(["crypto"], bot) is None
    policy["enabled"] = False
    path.write_text(json.dumps(policy))
    assert runner._current_history_year() is None


def test_unknown_policy_members_and_oversize_input_fail_closed(year_policy):
    path, policy = year_policy
    path.write_text(json.dumps({**policy, "ignored_year": 2014}))
    with pytest.raises(ValueError, match="invalid formal-history"):
        runner._current_history_year()
    path.write_text(" " * 8193)
    with pytest.raises(ValueError, match="bounded"):
        runner._current_history_year()


def test_scope_guard_runs_before_source_wait_or_model_work(year_policy, monkeypatch, tmp_path):
    states, emissions = [], []
    bot = SimpleNamespace(_rotate_error_log_if_needed=lambda: None,
        _artifact_backfill_status_path=lambda: tmp_path / "status.json",
        _record_artifact_maintenance_run=lambda status, **kw: states.append((status, kw)),
        _artifact_maintenance_markets=lambda: ["tw"],
        _resolve_market=lambda _: SimpleNamespace(day_trade_simulation_enabled=True))
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: bot)
    monkeypatch.setattr(runner, "_emit", lambda **kw: emissions.append(kw))
    monkeypatch.setattr(runner, "_wait_for_tw_public_refresh", lambda **kw: pytest.fail("unscoped owner must not wait or rebuild"))
    assert runner.run_once() == 0
    assert states[-1] == ("waiting_source", {"reason": "current_year_inference_owner_required", "attempted": 0, "deferred": 1})
    assert emissions[-1]["inference_started"] is False
    assert emissions[-1]["raw_data_deleted"] is False


@pytest.mark.parametrize("mode", ["tw_day_trade", "naive"])
def test_formal_history_command_passes_year_only_to_day_trade(year_policy, monkeypatch, tmp_path, mode):
    from services.discord_bot import bot
    cfg = SimpleNamespace(market="fixture", fold_id=11,
        config_path=str(tmp_path / "experiment.yaml"), output_dir=None)
    status = SimpleNamespace(data=SimpleNamespace(expected_latest_date="2026-11-10",
        last_data_date="2026-11-10", panel_date=None))
    dates, calls = iter(["2026-11-09", "2026-11-10"]), []
    monkeypatch.setattr(bot, "_formal_history_completed_date", lambda _: next(dates))
    monkeypatch.setattr(bot, "_preflight_formal_history_calendar", lambda _: None)
    monkeypatch.setattr(bot, "_formal_history_train_command", lambda: ["python", "train.py"])
    monkeypatch.setattr(bot, "_formal_history_timeout_seconds", lambda _: 600)
    monkeypatch.setattr(bot, "_market_execution_mode", lambda _: mode)
    monkeypatch.setattr(bot.subprocess, "run", lambda command, **kw:
        (calls.append(command) or SimpleNamespace(returncode=0)))
    assert bot._run_formal_history_backfill(cfg, status)
    command = calls[0]
    if mode == "tw_day_trade":
        assert command[command.index("--inference-year") + 1] == "2026"
    else:
        assert "--inference-year" not in command

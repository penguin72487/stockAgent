"""Cheap scheduling receipts remain distinct from full artifact validation."""

from __future__ import annotations

import json
import os
import fcntl
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from services.discord_bot import bot as discord_bot
from stockagent.live.market_config import load_market_config
from stockagent.training.lifecycle import TrainingArtifactLayout


@pytest.fixture
def completed_fold(tmp_path: Path, monkeypatch):
    layout = TrainingArtifactLayout(tmp_path)
    fold_dir = layout.fold_dir(11)
    fold_dir.mkdir()
    for path in layout.required_fold_paths(11):
        path.write_bytes(b"present-artifact")
    returns_path = fold_dir / "daily_portfolio_returns.parquet"
    pl.DataFrame({"date": ["2026-10-13", "2026-10-14"]}).write_parquet(returns_path)
    marker = {
        "status": "complete",
        "artifact_scope_version": 2,
        "fold_id": 11,
        "test_scope": "full_horizon",
        "deployment_scope": "stitched_deployment",
        "test_date_end": "2026-10-14",
    }
    marker_path = layout.fold_complete_path(11)
    marker_path.write_text(json.dumps(marker), encoding="utf-8")
    monkeypatch.setattr(discord_bot, "_market_fold_dir", lambda _cfg: fold_dir)
    monkeypatch.setattr(discord_bot, "_formal_returns_artifact_path", lambda _cfg: returns_path)
    cfg = SimpleNamespace(market="tw_day_trade", fold_id=11)
    return cfg, layout, marker, returns_path


def test_completed_fold_skips_inference_without_model_or_provider(completed_fold, monkeypatch):
    cfg, _, _, _ = completed_fold
    monkeypatch.setattr(
        discord_bot.subprocess, "run",
        lambda *_a, **_kw: pytest.fail("a complete workflow must not run train.py"),
    )
    monkeypatch.setattr(
        discord_bot, "generate_live_signal",
        lambda *_a, **_kw: pytest.fail("receipt read must not run a model"),
    )
    assert discord_bot._formal_history_completed_date(cfg) == "2026-10-14"
    status = SimpleNamespace(data=SimpleNamespace(
        expected_latest_date="2026-10-14", last_data_date="2026-10-14", panel_date=None,
    ))
    assert not discord_bot._run_formal_history_backfill(cfg, status)


@pytest.mark.parametrize("outcome", ["source_pending", "capacity_pending", "generic_failure", "forged_receipt"])
def test_formal_history_reclassifies_only_a_typed_child_source_receipt(
    tmp_path, monkeypatch, outcome,
):
    cfg = SimpleNamespace(market="fixture", config_path=str(tmp_path / "deployment.yaml"),
                          fold_id=1, output_dir=None)
    status = SimpleNamespace(data=SimpleNamespace(expected_latest_date="2026-10-29",
                                                   last_data_date="2026-10-29", panel_date=None))
    monkeypatch.setattr(discord_bot, "_formal_history_completed_date", lambda _: None)
    monkeypatch.setattr(discord_bot, "_preflight_formal_history_calendar", lambda _: None)
    monkeypatch.setattr(discord_bot, "_formal_history_timeout_seconds", lambda _: 30)
    root = tmp_path / "live" / "data_tw_public"
    stocks = root / "stocks"
    stocks.mkdir(parents=True)
    (root / "tw_corporate_action_reference.parquet").write_bytes(b"reference")
    lock = root.parent / ".locks" / "tw-public-refresh.lock"
    lock.parent.mkdir()
    lock.touch()
    original_run = subprocess.run
    child_code = """
import os, sys
from pathlib import Path
from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending, live_tw_public_source_read_guard
import train
outcome, stocks = sys.argv[1:]
if outcome == 'generic_failure':
    raise ValueError('real inference failure')
if outcome == 'forged_receipt':
    os.write(int(os.environ['STOCKAGENT_TW_SOURCE_PENDING_PIPE']), b'{"kind":"forged"}')
    raise ValueError('real inference failure')
def fail_under_lock():
    if outcome == 'capacity_pending':
        from stockagent.data_sync.completed_forward_capacity import CapacityDeferred
        raise CapacityDeferred('physical_backing_headroom_unconfirmed', required_bytes_including_reserve=12345)
    with live_tw_public_source_read_guard(Path(stocks)):
        raise AssertionError('writer lock bypassed')
train.main = fail_under_lock
train._run_entrypoint()
"""

    def substitute_child(_command, **kwargs):
        return original_run([sys.executable, "-c", child_code, outcome, str(stocks)],
                            capture_output=True, **kwargs)

    monkeypatch.setattr(discord_bot.subprocess, "run", substitute_child)
    with lock.open("a+") as producer:
        fcntl.flock(producer, fcntl.LOCK_EX)
        expected = (discord_bot.TWPublicSourceUpdatePending if outcome == "source_pending" else
                    discord_bot.CapacityDeferred if outcome == "capacity_pending" else discord_bot.BotUserError)
        with pytest.raises(expected) as caught:
            discord_bot._run_formal_history_backfill(cfg, status)
    if outcome == "source_pending":
        assert caught.value.details["producer_lock_held_at_child_observation"] is True
        assert caught.value.details["current_lock_state"] == "unverified"
    elif outcome == "capacity_pending":
        assert caught.value.details["reason"] == "physical_backing_headroom_unconfirmed"
        assert caught.value.details["required_bytes_including_reserve"] == 12345
        assert caught.value.details["current_capacity_state"] == "unverified"
        assert caught.value.details["allocation_admitted"] is False
    else:
        assert "formal history inference failed rc=" in str(caught.value)


@pytest.mark.parametrize("change", [
    {"test_date_end": "2026-09-30"},
    {"test_date_end": "2026-10-15"},
    {"status": "failed"},
    {"fold_id": 10},
    {"artifact_scope_version": 1},
    {"artifact_scope_version": float("inf")},
    {"test_scope": "subset"},
    {"deployment_scope": "full_horizon"},
])
def test_partial_or_incompatible_workflow_is_not_current(completed_fold, change):
    cfg, layout, marker, _ = completed_fold
    layout.fold_complete_path(11).write_text(json.dumps({**marker, **change}), encoding="utf-8")
    assert discord_bot._formal_history_latest_date(cfg) == "2026-10-14"
    assert discord_bot._formal_history_completed_date(cfg) is None


@pytest.mark.parametrize("payload", ["{", "[]", "null", "{}", " " * (64 * 1024) + "{}"])
def test_malformed_receipt_does_not_stop_scheduling(completed_fold, payload):
    cfg, layout, _, _ = completed_fold
    layout.fold_complete_path(11).write_text(payload, encoding="utf-8")
    assert discord_bot._formal_history_completed_date(cfg) is None


@pytest.mark.parametrize("name", [path.name for path in TrainingArtifactLayout(Path(".")).required_fold_paths(11)])
@pytest.mark.parametrize("state", ["missing", "empty"])
def test_missing_required_output_never_marks_workflow_complete(completed_fold, name, state):
    cfg, layout, _, _ = completed_fold
    path = layout.fold_dir(11) / name
    if state == "missing":
        path.unlink()
    else:
        path.write_bytes(b"")
    assert discord_bot._formal_history_completed_date(cfg) is None


def test_rewritten_returns_require_a_new_completion_receipt(completed_fold):
    cfg, layout, marker, returns_path = completed_fold
    marker_ns = layout.fold_complete_path(11).stat().st_mtime_ns
    os.utime(returns_path, ns=(marker_ns + 1, marker_ns + 1))
    assert discord_bot._formal_history_completed_date(cfg) is None
    layout.fold_complete_path(11).write_text(json.dumps(marker), encoding="utf-8")
    assert discord_bot._formal_history_completed_date(cfg) == "2026-10-14"


def test_selected_v8_cold_cache_budget_does_not_relax_activation_deadlines():
    path = Path(__file__).resolve().parents[1] / "services/discord_bot/markets/tw_day_trade_v8_annual_log_cash.yaml"
    cfg = load_market_config(path)
    assert cfg.formal_history_timeout_seconds == 3600
    assert cfg.pre_signal_timeout_seconds == 120
    assert cfg.completed_session_timeout_seconds == 600
    assert cfg.fold_id == 10
    assert cfg.config_path == "configs/deployments/tw_day_trade_v8_annual_log_cash_fold10.yaml"


@pytest.fixture
def factorized_calendar(tmp_path, monkeypatch):
    config_path = tmp_path / "deployment.yaml"
    config_path.write_text("fixture", encoding="utf-8")
    parquet_root = tmp_path / "stocks"
    parquet_root.mkdir()
    pl.DataFrame({"date": ["2013-12-31", "2014-01-06", "2026-10-13", "2026-10-14"]}).write_parquet(
        parquet_root / "2330_features.parquet"
    )
    manifest_path = tmp_path / "factorized_manifest.json"
    manifest_path.write_text(json.dumps({"dates": ["2026-10-13", "2026-10-14"]}), encoding="utf-8")
    train_config = SimpleNamespace(data=SimpleNamespace(
        factorized_feature_manifest=str(manifest_path), benchmark_name="2330",
        parquet_root=str(parquet_root), panel_start_date="2014-01-06",
    ))
    monkeypatch.setattr(discord_bot, "load_config", lambda _path: train_config)
    cfg = SimpleNamespace(market="factorized", config_path=str(config_path), fold_id=11)
    return cfg, train_config, manifest_path


def test_partial_live_projection_never_launches_full_formal_inference(factorized_calendar, monkeypatch):
    cfg, _, _ = factorized_calendar
    monkeypatch.setattr(discord_bot, "_formal_history_completed_date", lambda _: None)
    monkeypatch.setattr(
        discord_bot.subprocess, "run",
        lambda *_a, **_kw: pytest.fail("proven wrong-scope input must not start train.py"),
    )
    status = SimpleNamespace(data=SimpleNamespace(
        expected_latest_date="2026-10-14", last_data_date="2026-10-14", panel_date=None,
    ))
    with pytest.raises(discord_bot.BotUserError, match="omits 1 benchmark sessions"):
        discord_bot._run_formal_history_backfill(cfg, status)


def test_preflight_preserves_configured_full_history_scope(factorized_calendar):
    cfg, train_config, manifest_path = factorized_calendar
    manifest_path.write_text(json.dumps({"dates": ["2014-01-06", "2026-10-13", "2026-10-14"]}))
    assert discord_bot._preflight_formal_history_calendar(cfg) is None
    assert train_config.data.panel_start_date == "2014-01-06"


def test_preflight_rejects_missing_checkpoint_year_before_inference(factorized_calendar, monkeypatch):
    cfg, train_config, manifest_path = factorized_calendar
    train_config.data.panel_start_date = "2026-01-01"
    train_config.walk_forward = SimpleNamespace(year_boundary_mode="calendar")
    train_config.training = SimpleNamespace(lookback=32)
    checkpoint = Path(cfg.config_path).with_name("checkpoint_best.pt")
    monkeypatch.setattr(discord_bot, "_market_model_checkpoint", lambda _: checkpoint)
    monkeypatch.setattr(
        "stockagent.training.runtime.load_checkpoint",
        lambda _: {"fold_id": 11, "train_years": list(range(2014, 2025)),
                   "val_years": [2025], "test_years": [2026]},
    )
    monkeypatch.setattr(discord_bot, "_formal_history_completed_date", lambda _: None)
    monkeypatch.setattr(discord_bot.subprocess, "run", lambda *_a, **_kw: pytest.fail("must not start trainer"))
    status = SimpleNamespace(data=SimpleNamespace(
        expected_latest_date="2026-10-14", last_data_date="2026-10-14", panel_date=None,
    ))
    with pytest.raises(discord_bot.BotUserError, match=r"missing_val_years=\[2025\]"):
        discord_bot._run_formal_history_backfill(cfg, status)


def test_preflight_accepts_saved_checkpoint_years_without_renumbering(factorized_calendar, monkeypatch):
    cfg, train_config, manifest_path = factorized_calendar
    train_config.data.panel_start_date = "2014-01-06"
    train_config.walk_forward = SimpleNamespace(year_boundary_mode="calendar")
    train_config.training = SimpleNamespace(lookback=32)
    manifest_path.write_text(json.dumps({"dates": ["2014-01-06", "2025-01-02", "2026-10-13", "2026-10-14"]}))
    monkeypatch.setattr(discord_bot, "_market_model_checkpoint", lambda _: Path("checkpoint_best.pt"))
    monkeypatch.setattr(
        "stockagent.training.runtime.load_checkpoint",
        lambda _: {"fold_id": 11, "train_years": list(range(2014, 2025)),
                   "val_years": [2025], "test_years": [2026]},
    )
    assert discord_bot._preflight_formal_history_calendar(cfg) is None


def test_preflight_rejects_mismatched_requested_fold(factorized_calendar, monkeypatch):
    cfg, train_config, manifest_path = factorized_calendar
    train_config.walk_forward = SimpleNamespace(year_boundary_mode="calendar")
    train_config.training = SimpleNamespace(lookback=32)
    manifest_path.write_text(json.dumps({"dates": ["2014-01-06", "2025-01-02", "2026-10-13", "2026-10-14"]}))
    monkeypatch.setattr(discord_bot, "_market_model_checkpoint", lambda _: Path("checkpoint_best.pt"))
    monkeypatch.setattr(
        "stockagent.training.runtime.load_checkpoint",
        lambda _: {"fold_id": 10, "train_years": list(range(2014, 2025)),
                   "val_years": [2025], "test_years": [2026]},
    )
    with pytest.raises(discord_bot.BotUserError, match="requested fold=11 differs from checkpoint fold=10"):
        discord_bot._preflight_formal_history_calendar(cfg)


@pytest.mark.parametrize("dates", [[], [None], [["2026-10-14"]], ["not-a-date"],
                                    ["2026-10-14", "2026-10-13"], ["2026-10-14"] * 2])
def test_preflight_rejects_invalid_calendar_without_model(factorized_calendar, dates):
    cfg, _, manifest_path = factorized_calendar
    manifest_path.write_text(json.dumps({"dates": dates}))
    with pytest.raises(discord_bot.BotUserError, match="input calendar is invalid"):
        discord_bot._preflight_formal_history_calendar(cfg)


@pytest.mark.parametrize("case", ["no_factorized", "non_benchmark_calendar", "absent_config"])
def test_unknown_preflight_scope_is_left_to_canonical_trainer(factorized_calendar, case):
    cfg, train_config, _ = factorized_calendar
    if case == "no_factorized":
        train_config.data.factorized_feature_manifest = ""
    elif case == "non_benchmark_calendar":
        train_config.data.benchmark_name = "universe_average_return"
    else:
        cfg.config_path = str(Path(cfg.config_path).with_name("absent.yaml"))
    assert discord_bot._preflight_formal_history_calendar(cfg) is None


@pytest.fixture
def ready_job(tmp_path, monkeypatch):
    path = tmp_path / "status.json"
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH", str(path))
    key, market = "2026-10-14:mode:artifact_backfill", "mode"
    job = {"key": key, "market": market, "status": "ready", "attempt": 2,
           "run_id": "prior-owner", "completed_at": "2026-10-14T16:00:00+08:00",
           "next_retry_at": None}
    path.write_text(json.dumps({"jobs": {key: job}}))
    return path, key, market, job


def test_failed_readback_invalidates_only_obsolete_ready_hint(ready_job):
    path, key, market, job = ready_job
    assert discord_bot._invalidate_ready_artifact_backfill(key, market)
    current = json.loads(path.read_text())["jobs"][key]
    assert current["status"] == "failed"
    assert current["error_type"] == "ArtifactCompletionProofError"
    assert current["prior_ready_receipt"] == {name: job[name] for name in ("status", "attempt", "run_id", "completed_at")}
    assert discord_bot._artifact_backfill_retry_allowed(key)
    assert current["attempt"] == 2
    assert not discord_bot._invalidate_ready_artifact_backfill(key, market)


@pytest.mark.parametrize("case", ["running", "failed", "other_market"])
def test_invalidation_never_overwrites_concurrent_work_or_backoff(ready_job, case):
    path, key, market, job = ready_job
    if case == "other_market":
        job["market"] = "other"
    else:
        job["status"] = case
    path.write_text(json.dumps({"jobs": {key: job}}))
    before = path.read_bytes()
    assert not discord_bot._invalidate_ready_artifact_backfill(key, market)
    assert path.read_bytes() == before

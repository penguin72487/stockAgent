"""Receipt-only close refresh; no substitution for a missing projection date."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from scripts import run_discord_artifact_maintenance as runner
from stockagent.live.market_config import LiveMarketConfig


@pytest.fixture
def stale(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.delenv("STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT", raising=False)
    completed = tmp_path / "artifacts/data_refresh/tw_public/completed_session/latest.json"
    completed.parent.mkdir(parents=True)
    completed.write_text(json.dumps({"status": "ok", "expected_date": "2020-03-03",
                                      "after": {"current": True}}))
    root = tmp_path / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1"
    path = root / "current/factorized_manifest.json"
    path.parent.mkdir(parents=True)
    manifest = {"status": "complete", "research_only": True,
                "historical_point_in_time": False, "feature_lag": 1,
                "dates": ["2020-03-02"],
                "paper_inference": {"status": "verified", "raw_source_publication": False,
                                    "contract": "factorized_paper_projection_lag1_v1",
                                    "decision_session": "2020-03-03"}}
    path.write_text(json.dumps(manifest))
    cfg = LiveMarketConfig(market="factorized", label="factorized", config_path="", completed_session_command=(
        "scripts/update_tw_day_trade_factorized_live.py", "--phase", "completed"))
    return cfg, completed, path, manifest


def test_decision_session_label_does_not_certify_close_calendar(stale):
    _cfg, _completed, path, _manifest = stale
    hint = runner._stale_factorized_projection_request()
    assert hint["expected_completed_session"] == "2020-03-03"
    assert hint["projection_end"] == "2020-03-02"
    assert hint["manifest"] == str(path)
    assert hint["advisory_only"] is True and hint["model_executed"] is False


def test_receipt_only_controller_enqueues_only_the_enabled_canonical_source_owner(stale, monkeypatch):
    _cfg, _completed, _path, _manifest = stale
    markets = runner.ROOT / "services/discord_bot/markets"
    markets.mkdir(parents=True)
    monkeypatch.setenv("STOCKAGENT_MARKETS_DIR", str(markets))
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", "all")
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
                       str(runner.ROOT / "artifacts/discord_bot/artifact_backfill_status.json"))
    (markets / "owned.yaml").write_text(
        "market: owned\nenabled: true\ncompleted_session_command:\n"
        "- scripts/update_tw_day_trade_factorized_live.py\n- --phase\n- completed\n")
    (markets / "other.yaml").write_text("market: other\nenabled: true\n")
    (markets / "disabled.yaml").write_text(
        "market: disabled\nenabled: false\ncompleted_session_command:\n"
        "- scripts/update_tw_day_trade_factorized_live.py\n")
    result = runner.retry_probe()
    assert result["due"] is True
    assert result["due_keys"] == ["source_projection_refresh:owned"]
    assert result["advisory_only"] is True


@pytest.mark.parametrize("change", ["failed", "missing", "not_current"])
def test_no_official_completed_receipt_means_no_refresh_hint(stale, change):
    _cfg, completed, _path, _manifest = stale
    if change == "missing":
        completed.unlink()
    else:
        completed.write_text(json.dumps({"status": change, "expected_date": "2020-03-03",
                                        "after": {"current": change != "not_current"}}))
    assert runner._stale_factorized_projection_request() is None


@pytest.mark.parametrize("change", ["status", "research_only", "historical_point_in_time",
                                   "feature_lag", "paper_contract", "raw_source_publication"])
def test_invalid_projection_abi_cannot_request_recovery(stale, change):
    _cfg, _completed, path, manifest = stale
    if change == "paper_contract":
        manifest["paper_inference"]["contract"] = "unknown"
    elif change == "raw_source_publication":
        manifest["paper_inference"][change] = True
    else:
        manifest[change] = "unknown"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="unverified factorized"):
        runner._stale_factorized_projection_request()


@pytest.mark.parametrize("dates", [[], ["2020-03-02", "2020-03-02"],
                                   ["2020-03-03", "2020-03-02"], [None], [["2020-03-02"]]])
def test_bad_calendar_is_a_hard_error_not_a_fill_or_refresh(stale, dates):
    _cfg, _completed, path, manifest = stale
    manifest["dates"] = dates
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="unique ordered"):
        runner._stale_factorized_projection_request()


def test_current_projection_has_no_refresh_request(stale):
    _cfg, _completed, path, manifest = stale
    manifest["dates"].append("2020-03-03")
    path.write_text(json.dumps(manifest))
    assert runner._stale_factorized_projection_request() is None


def test_missing_projection_is_a_source_recovery_request_not_a_silent_idle(stale):
    _cfg, _completed, path, _manifest = stale
    path.unlink()
    hint = runner._stale_factorized_projection_request()
    assert hint["reason"] == "published_projection_missing"
    assert hint["expected_completed_session"] == "2020-03-03"
    assert hint["status"] == "projection_refresh_required" and hint["model_executed"] is False


def test_current_tail_still_requests_the_selected_formal_history_scope(stale, monkeypatch):
    from stockagent import config as config_module

    _cfg, _completed, path, manifest = stale
    manifest["dates"].append("2020-03-03")
    manifest["paper_inference"]["source_identity"] = {"start_date": "2020-03-02"}
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr(config_module, "load_config", lambda _: SimpleNamespace(
        data=SimpleNamespace(panel_start_date="2014-01-06")))
    # A metadata-only retry probe needs no config/model import.
    assert runner._stale_factorized_projection_request() is None
    hint = runner._stale_factorized_projection_request(config_path="selected.yaml")
    assert hint["reason"] == "formal_history_scope_missing"
    assert hint["required_start_date"] == "2014-01-06"
    assert hint["projection_source_start"] == "2020-03-02"


def test_requested_non_session_start_does_not_cause_endless_refresh(stale, monkeypatch):
    from stockagent import config as config_module

    _cfg, _completed, path, manifest = stale
    manifest["dates"].append("2020-03-03")
    manifest["paper_inference"]["source_identity"] = {"start_date": "2020-03-01"}
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr(config_module, "load_config", lambda _: SimpleNamespace(
        data=SimpleNamespace(panel_start_date="2020-03-01")))
    assert runner._stale_factorized_projection_request(config_path="selected.yaml") is None


def test_future_completed_clock_is_not_accepted(stale):
    _cfg, completed, _path, _manifest = stale
    completed.write_text(json.dumps({"status": "ok", "expected_date": "2999-01-01",
                                    "after": {"current": True}}))
    with pytest.raises(ValueError, match="future completed"):
        runner._stale_factorized_projection_request()


def test_projection_pointer_cannot_escape_its_owner(stale, tmp_path):
    _cfg, _completed, path, manifest = stale
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(manifest))
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(ValueError, match="escapes"):
        runner._stale_factorized_projection_request()


def test_cheap_bridge_defers_stale_close_before_loading_model_owner(stale, monkeypatch):
    cfg, _completed, _path, _manifest = stale
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: pytest.fail("no owner import in bridge"))
    emissions = []
    monkeypatch.setattr(runner, "_emit", lambda **event: emissions.append(event))
    assert runner._recover_deferred_factorized_source(cfg, allow_warming=False) is False
    assert emissions[-1]["status"] == "factorized_projection_refresh_deferred"
    assert emissions[-1]["model_executed"] is False


@pytest.mark.parametrize("blocked", ["market_open", "publication"])
def test_stale_calendar_recovery_keeps_actual_market_and_publication_gates(stale, monkeypatch, blocked):
    cfg, _completed, _path, _manifest = stale
    bot = SimpleNamespace(
        _ensure_signal_ready=lambda _: SimpleNamespace(market_open=blocked == "market_open"),
        _completed_session_publication_ready=lambda _: blocked != "publication",
        _run_completed_session_command=lambda *_a, **_kw: pytest.fail("source gate bypass"))
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: bot)
    assert runner._recover_deferred_factorized_source(cfg) is False


@pytest.mark.parametrize("refresh", [True, False])
def test_source_receipt_and_real_calendar_are_both_required_after_recovery(stale, monkeypatch, refresh):
    cfg, _completed, path, manifest = stale
    source_receipt = runner._factorized_source_updater_receipt()
    calls = []

    def command(got, *, bypass_cache):
        assert got is not cfg and bypass_cache is True
        assert got.completed_session_command == cfg.completed_session_command
        assert got.completed_session_timeout_seconds == runner.FACTORIZED_SOURCE_RECOVERY_SECONDS
        calls.append("configured_canonical_updater")
        source_receipt.write_text('{"status":"verified"}')
        if refresh:
            manifest["dates"].append("2020-03-03")
            path.write_text(json.dumps(manifest))

    bot = SimpleNamespace(_ensure_signal_ready=lambda _: SimpleNamespace(market_open=False),
                          _completed_session_publication_ready=lambda _: True,
                          _run_completed_session_command=command)
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: bot)
    if refresh:
        assert runner._recover_deferred_factorized_source(cfg)
        assert runner._recover_deferred_factorized_source(cfg)
    else:
        with pytest.raises(RuntimeError, match="still lacks"):
            runner._recover_deferred_factorized_source(cfg)
    assert calls == ["configured_canonical_updater"]


def test_preopen_missing_projection_hint_uses_the_same_verified_offhours_recovery(stale, monkeypatch):
    cfg, _completed, path, manifest = stale
    path.unlink()
    request = runner._factorized_source_updater_receipt()
    request.parent.mkdir(exist_ok=True)
    request.write_text(json.dumps({"status": "projection_refresh_required", "phase": "preopen",
        "error_type": "FactorizedPreopenProjectionPending", "model_executed": False,
        "last_good_generation_retained": True}))
    calls = []
    def command(got, *, bypass_cache):
        assert bypass_cache is True
        assert got.completed_session_timeout_seconds == runner.FACTORIZED_SOURCE_RECOVERY_SECONDS
        calls.append("same_canonical_source")
        manifest["dates"].append("2020-03-03")
        path.write_text(json.dumps(manifest))
        request.write_text('{"status":"verified"}')
    bot = SimpleNamespace(_ensure_signal_ready=lambda _: SimpleNamespace(market_open=False),
        _completed_session_publication_ready=lambda _: True, _run_completed_session_command=command)
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: bot)
    assert runner._recover_deferred_factorized_source(cfg)
    assert runner._recover_deferred_factorized_source(cfg)
    assert calls == ["same_canonical_source"]

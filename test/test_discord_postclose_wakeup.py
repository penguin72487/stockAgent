from __future__ import annotations

from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import discord_postclose_wakeup as wakeup
from scripts import run_discord_artifact_maintenance as runner


@pytest.fixture
def scope(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    markets = root / "markets"
    markets.mkdir(parents=True)
    completed = root / "close.json"
    completed.write_text('{"status":"ok","after":{"current":true}}')
    stocks = root / "source/stocks"
    stocks.mkdir(parents=True)
    (stocks / "2330_features.parquet").write_bytes(b"source-not-decoded")
    config = root / "model.yaml"
    base = Path(__file__).resolve().parents[1] / "configs/markets/tw.yaml"
    config.write_text(f"base_config: {base}\ndata:\n  parquet_root: {stocks}\n")
    (markets / "tw.yaml").write_text(
        f"market: tw\nmarket_type: tw\nenabled: true\nconfig_path: {config}\n"
        f"day_trade_rule_data_dir: {stocks.parent}\ncheckpoint_path: checkpoint.pt\n")
    monkeypatch.setenv("STOCKAGENT_MARKETS_DIR", str(markets))
    monkeypatch.setenv("STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT", str(completed))
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", "tw")
    monkeypatch.setenv("STOCKAGENT_TW_PUBLIC_REFRESH_LOCK", str(root / "source.lock"))
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_RETRY_STATE_PATH", str(root / "dispatch/latest.json"))
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH", str(root / "jobs.json"))
    monkeypatch.delenv("STOCKAGENT_POSTCLOSE_WAKEUP_STATUS_PATH", raising=False)
    monkeypatch.setattr(runner, "ROOT", root)
    return root, completed, stocks, markets


def permit(monkeypatch):
    from scripts import check_outside_tw_opening_resource_window as window

    monkeypatch.setattr(window, "evaluate", lambda *a, **kw: {"allowed": True})
    calls = []
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **kw: calls.append((a, kw)))
    return calls


def test_observation_is_stable_nonmutating_and_tracks_in_place_source_changes(scope):
    root, completed, stocks, _markets = scope
    original = completed.read_bytes()
    first = wakeup.observe(root)
    assert first == wakeup.observe(root)
    assert first["markets"] == ["tw"] and first["advisory_only"]
    (stocks / "2330_features.parquet").write_bytes(b"changed-in-place")
    assert wakeup.observe(root)["fingerprint"] != first["fingerprint"]
    assert completed.read_bytes() == original
    assert not wakeup.accepted_path(root).exists()


@pytest.mark.parametrize("change", ["new_symbol", "eligibility", "checkpoint", "config"])
def test_relevant_changes_wake_even_when_the_close_date_is_unchanged(scope, change):
    root, _close, stocks, _markets = scope
    first = wakeup.observe(root)["fingerprint"]
    target = {"new_symbol": stocks / "2317_features.parquet",
              "eligibility": stocks.parent / "twse_day_trade_eligibility.parquet",
              "checkpoint": root / "checkpoint.pt", "config": root / "model.yaml"}[change]
    if change == "config":
        target.write_text(target.read_text() + "experiment_name: changed\n")
    else:
        target.write_bytes(b"new")
    assert wakeup.observe(root)["fingerprint"] != first


@pytest.mark.parametrize("state", [{}, {"status": "ok"}, {"status": "failed", "after": {"current": True}}])
def test_unaccepted_close_never_dispatches(scope, monkeypatch, state):
    _root, close, _stocks, _markets = scope
    close.write_text(json.dumps(state))
    calls = permit(monkeypatch)
    assert runner.request_due_postclose_cache() == 0 and not calls


def test_worker_readback_not_dispatch_acceptance_suppresses_future_wakeups(scope, monkeypatch, capsys):
    root, _close, _stocks, _markets = scope
    calls = permit(monkeypatch)
    assert runner.request_due_postclose_cache() == 0
    assert len(calls) == 1 and calls[0][0][0][-1] == wakeup.OWNER_UNIT
    assert not wakeup.accepted_path(root).exists()
    assert runner.request_due_postclose_cache() == 0 and len(calls) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["reason"] == "unchanged_postclose_wakeup_coalesced"
    path = wakeup.accepted_path(root)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({**wakeup.observe(root), "status": "canonical_artifacts_reusable",
                               "artifact_generations": wakeup._generations([str(root / "model.yaml")])}))
    before = path.read_bytes()
    assert runner.request_due_postclose_cache() == 0 and len(calls) == 1
    assert path.read_bytes() == before


@pytest.mark.parametrize("age", [301, -10])
def test_failed_or_interrupted_dispatch_recovers_and_clock_rollback_cannot_stall(scope, monkeypatch, age):
    root, _close, _stocks, _markets = scope
    calls = permit(monkeypatch)
    path = runner._retry_dispatch_state_path().with_name("postclose_dispatch_latest.json")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({**wakeup.observe(root), "dispatch_accepted_at":
                              (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat()}))
    assert runner.request_due_postclose_cache() == 0 and len(calls) == 1


@pytest.mark.parametrize("lock", ["source.lock", "postclose_signal_cache.worker.lock", "jobs.json.worker.lock"])
def test_real_owner_lock_prevents_controller_duplicates_without_creating_receipts(scope, monkeypatch, lock):
    root, _close, _stocks, _markets = scope
    calls = permit(monkeypatch)
    with (root / lock).open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert runner.request_due_postclose_cache() == 0 and not calls
    assert not runner._retry_dispatch_state_path().exists()
    assert not wakeup.accepted_path(root).exists()


def test_shared_source_reader_is_not_mistaken_for_an_exclusive_producer(scope, monkeypatch):
    root, *_ = scope
    calls = permit(monkeypatch)
    with (root / "source.lock").open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
        assert not wakeup.lock_held(root / "source.lock", producer_only=True)
        assert runner.request_due_postclose_cache() == 0 and len(calls) == 1


@pytest.mark.parametrize("hour,minute,allowed", [(7, 30, True), (8, 6, True), (8, 7, False), (13, 39, False), (13, 40, True)])
def test_short_owner_uses_its_own_budget_not_the_full_history_budget(hour, minute, allowed):
    from scripts.check_outside_tw_opening_resource_window import evaluate
    from datetime import time
    from zoneinfo import ZoneInfo

    now = datetime(2026, 10, 15, hour, minute, tzinfo=ZoneInfo("Asia/Taipei"))
    assert evaluate(now, minimum_runway_minutes=wakeup.RUNWAY_MINUTES,
                    protected_until=time(13, 40))["allowed"] is allowed
    if hour == 7:
        assert not evaluate(now, minimum_runway_minutes=runner.ARTIFACT_RETRY_RUNWAY_MINUTES,
                            protected_until=time(13, 40))["allowed"]


def bot_fixture(root):
    weights = root / "weights.parquet"
    weights.write_bytes(b"weights")
    (root / "summary.json").write_text("{}")
    cfg = SimpleNamespace(config_path="unused")
    return SimpleNamespace(
        _effective_market_config=lambda value: value, _resolve_market=lambda _: cfg,
        load_config=lambda _: SimpleNamespace(data=SimpleNamespace(parquet_root=root / "source/stocks")),
        _ensure_signal_ready=lambda _: SimpleNamespace(data=SimpleNamespace(fresh=True)),
        _completed_session_signal_path=lambda *_: True, _completed_session_receipt_ready=lambda _: True,
        _latest_market_signal=lambda _: (root / "summary.json", {}),
        _can_reuse_latest_signal_now=lambda *a, **kw: (True, "canonical_reuse"),
        _summary_artifact_path=lambda *a: weights,
    )


def test_only_complete_canonical_readback_acknowledges_the_unchanged_generation(scope):
    root, *_ = scope
    bot = bot_fixture(root)
    snapshot = wakeup.observe(root)
    assert wakeup.acknowledge(root, snapshot, bot)
    assert wakeup.read_json(wakeup.accepted_path(root))["status"] == "canonical_artifacts_reusable"


@pytest.mark.parametrize("failure", ["stale", "not_completed", "no_receipt", "no_latest", "not_reusable", "preview", "missing_weights", "generation_changed"])
def test_noop_partial_or_changed_sources_cannot_be_acknowledged(scope, failure):
    root, _close, stocks, _markets = scope
    bot = bot_fixture(root)
    snapshot = wakeup.observe(root)
    if failure == "stale":
        bot._ensure_signal_ready = lambda _: SimpleNamespace(data=SimpleNamespace(fresh=False))
    elif failure == "not_completed":
        bot._completed_session_signal_path = lambda *_: False
    elif failure == "no_receipt":
        bot._completed_session_receipt_ready = lambda _: False
    elif failure == "no_latest":
        bot._latest_market_signal = lambda _: None
    elif failure == "not_reusable":
        bot._can_reuse_latest_signal_now = lambda *a, **kw: (False, "changed")
    elif failure == "preview":
        bot._latest_market_signal = lambda _: (root / "summary.json", {"postclose_fast_preview": True})
    elif failure == "missing_weights":
        (root / "weights.parquet").unlink()
    else:
        (stocks / "2330_features.parquet").write_bytes(b"late change")
    assert not wakeup.acknowledge(root, snapshot, bot)
    assert not wakeup.accepted_path(root).exists()


def test_fresh_process_entire_observation_has_no_torch_pandas_or_provider_import(scope):
    root, *_ = scope
    code = """
import json, resource, sys, time
from pathlib import Path
t=time.perf_counter(); c=time.process_time()
from scripts import discord_postclose_wakeup as wakeup
snapshot=wakeup.observe(Path(sys.argv[1]))
assert snapshot['fingerprint'] and not {'torch','pandas','requests'} & sys.modules.keys()
print(json.dumps({'wall': time.perf_counter()-t, 'cpu': time.process_time()-c,
 'rss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,'snapshot':snapshot}))
"""
    result = subprocess.run([sys.executable, "-c", code, str(root)], env=dict(os.environ),
                            text=True, capture_output=True, timeout=20, check=True)
    assert json.loads(result.stdout)["snapshot"]["markets"] == ["tw"]


def test_same_controller_keeps_both_original_owners(monkeypatch):
    calls = []
    monkeypatch.setattr(runner, "request_due_postclose_cache", lambda: calls.append("close") or 0)
    monkeypatch.setattr(runner, "request_due_retry", lambda: calls.append("history") or 0)
    assert runner.request_due_work() == 0 and calls == ["close", "history"]


def test_unchanged_plan_is_exact_and_does_not_reparse_configs(scope, monkeypatch):
    import stockagent.config

    root, *_ = scope
    original = wakeup.observe(root)
    monkeypatch.setattr(stockagent.config, "load_config", lambda *_a, **_kw: pytest.fail("unchanged plan parsed YAML"))
    assert wakeup.observe(root, previous=original) == original
    assert wakeup.observe(root, previous=[{}, original]) == original


@pytest.mark.parametrize("change", ["symbol_in_place", "new_symbol", "inherited_yaml", "new_code", "selection_env"])
def test_observation_plan_cannot_hide_a_changed_dependency(scope, monkeypatch, change):
    root, _close, stocks, _markets = scope
    base = root / "base.yaml"
    base.write_text((root / "model.yaml").read_text())
    (root / "model.yaml").write_text(f"base_config: {base}\n")
    code_dir = root / "stockagent/data"
    code_dir.mkdir(parents=True)
    before = wakeup.observe(root)
    if change == "symbol_in_place":
        (stocks / "2330_features.parquet").write_bytes(b"in-place changed")
    elif change == "new_symbol":
        (stocks / "2317_features.parquet").write_bytes(b"new")
    elif change == "inherited_yaml":
        base.write_text(base.read_text() + "experiment_name: different\n")
    elif change == "new_code":
        (code_dir / "new_feature.py").write_text("# different implementation\n")
    else:
        monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", "none")
    after = wakeup.observe(root, previous=before)
    assert after["fingerprint"] != before["fingerprint"]
    assert after == wakeup.observe(root)


def test_artifact_damage_cannot_be_hidden_by_an_unchanged_source_plan(scope):
    root, *_ = scope
    bot = bot_fixture(root)
    assert wakeup.acknowledge(root, wakeup.observe(root), bot)
    accepted = wakeup.read_json(wakeup.accepted_path(root))
    assert wakeup.artifacts_unchanged(accepted)
    (root / "weights.parquet").write_bytes(b"changed")
    assert not wakeup.artifacts_unchanged(accepted)


def test_unchanged_plan_fresh_process_imports_no_data_or_model_runtime(scope):
    root, *_ = scope
    path = root / "plan.json"
    path.write_text(json.dumps(wakeup.observe(root)))
    code = """
import json, sys
from pathlib import Path
from scripts import discord_postclose_wakeup as w
root=Path(sys.argv[1]); old=json.loads((root/'plan.json').read_bytes())
assert w.observe(root,previous=old)==old
assert not {'torch','pandas','pyarrow','numpy','stockagent.config','requests'} & sys.modules.keys()
"""
    subprocess.run([sys.executable, "-c", code, str(root)], env=dict(os.environ),
                   text=True, capture_output=True, timeout=10, check=True)


def test_scoped_installer_retains_the_existing_gateway_and_path_owners():
    text = Path("scripts/install_discord_bot_service.sh").read_text()
    assert "--retry-only" in text
    assert 'unit_names=("$RETRY_SERVICE_NAME" "$RETRY_TIMER_NAME" "$POSTCLOSE_CACHE_SERVICE_NAME")' in text
    assert "gateway_restarted=false path_restarted=false" in text


def test_cache_native_event_owner_cannot_cross_the_market_resource_fence():
    unit = Path("deploy/systemd/stockagent-discord-postclose-cache.service.in").read_text()
    assert "--minimum-runway-minutes 13 --protected-until 13:40" in unit
    assert "TimeoutStartSec=10m" in unit and "TimeoutStopSec=90s" in unit

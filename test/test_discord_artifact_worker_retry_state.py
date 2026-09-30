from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import run_discord_artifact_maintenance as runner


@pytest.fixture
def isolated_worker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    status_path = tmp_path / "artifact_backfill_status.json"
    emissions: list[dict] = []
    bot = runner.discord_bot
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH", str(status_path))
    monkeypatch.setattr(bot, "_rotate_error_log_if_needed", lambda: None)
    monkeypatch.setattr(bot, "_opening_critical_work_pending", lambda: False)
    monkeypatch.setattr(bot, "_interactive_signal_work_pending", lambda: False)
    monkeypatch.setattr(runner, "_wait_for_tw_public_refresh", lambda: True)
    monkeypatch.setattr(runner, "_populate_postclose_signal_caches", lambda _: (0, 0))
    monkeypatch.setattr(bot, "_artifact_maintenance_markets", lambda: ["market"])
    monkeypatch.setattr(
        bot,
        "_resolve_market",
        lambda market: SimpleNamespace(
            market=market, timezone="Asia/Taipei", day_trade_simulation_enabled=False
        ),
    )
    monkeypatch.setattr(bot, "_artifact_backfill_key", lambda cfg, _: cfg.market)
    monkeypatch.setattr(bot, "_market_has_model", lambda _: True)
    monkeypatch.setattr(bot, "_reconcile_artifact_backfill_if_current", lambda *a, **kw: False)
    monkeypatch.setattr(
        bot,
        "_run_artifact_backfill_sync",
        lambda _: pytest.fail("deferred work must not bypass retry backoff"),
    )
    monkeypatch.setattr(runner, "_emit", lambda **row: emissions.append(row))
    return status_path, emissions


def _pending_job(state: str) -> dict:
    return {
        "key": "market",
        "market": "market",
        "status": state,
        "run_id": runner.discord_bot._BOT_RUN_ID,
        "attempt": 4,
        "next_retry_at": (
            datetime.now().astimezone() + timedelta(hours=1)
        ).isoformat(),
        "error_type": "FileNotFoundError" if state == "failed" else None,
        "error_message": "retained source is missing" if state == "failed" else None,
    }


@pytest.mark.parametrize("state", ["failed", "ready", "running"])
def test_unreconciled_skip_is_not_worker_completion(isolated_worker, state: str) -> None:
    status_path, emissions = isolated_worker
    job = _pending_job(state)
    status_path.write_text(json.dumps({"schema_version": 2, "jobs": {"market": job}}))

    assert runner.run_once() == 0

    status = json.loads(status_path.read_text())
    assert status["jobs"]["market"] == job
    assert status["maintenance_run"]["status"] == "waiting_source"
    assert status["maintenance_run"]["reason"] == "artifact_retry_deferred"
    assert status["maintenance_run"]["attempted"] == 0
    assert status["maintenance_run"]["deferred"] == 1
    assert emissions[-1]["retry_deferred"] == 1
    assert any(row.get("reason") == "artifact_retry_deferred" for row in emissions)
    expected_health = "degraded" if state == "failed" else "running" if state == "running" else "waiting_source"
    assert runner.discord_bot._artifact_backfill_health_summary()["status"] == expected_health


def test_proven_recovery_can_complete_without_retry(isolated_worker, monkeypatch) -> None:
    status_path, emissions = isolated_worker
    status_path.write_text(json.dumps({"jobs": {"market": _pending_job("failed")}}))

    def reconciled(_cfg, *, key, market):
        runner.discord_bot._finish_artifact_backfill(key, market, status="ready")
        return True

    monkeypatch.setattr(
        runner.discord_bot, "_reconcile_artifact_backfill_if_current", reconciled
    )

    assert runner.run_once() == 0
    status = json.loads(status_path.read_text())
    assert status["jobs"]["market"]["status"] == "ready"
    assert status["maintenance_run"]["status"] == "complete"
    assert status["maintenance_run"]["deferred"] == 0
    assert emissions[-1]["retry_deferred"] == 0


def test_other_markets_continue_without_hiding_deferred_work(
    isolated_worker, monkeypatch, tmp_path
) -> None:
    status_path, emissions = isolated_worker
    pending = _pending_job("failed")
    status_path.write_text(json.dumps({"jobs": {"market": pending}}))
    monkeypatch.setattr(
        runner.discord_bot, "_artifact_maintenance_markets", lambda: ["market", "other"]
    )
    calls: list[str] = []

    def inferred(cfg):
        calls.append(cfg.market)
        return SimpleNamespace(summary={"panel_date": "2026-09-30"}, output_dir=tmp_path)

    monkeypatch.setattr(runner.discord_bot, "_run_artifact_backfill_sync", inferred)

    assert runner.run_once() == 0
    status = json.loads(status_path.read_text())
    assert calls == ["other"]
    assert status["jobs"]["market"] == pending
    assert status["jobs"]["other"]["status"] == "ready"
    assert status["maintenance_run"]["status"] == "waiting_source"
    assert status["maintenance_run"]["attempted"] == 1
    assert emissions[-1]["deferred"] == 1

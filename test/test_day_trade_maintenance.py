import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import subprocess
import sys

import pytest

from stockagent.live.day_trade_maintenance import control_lease
from scripts import switch_tw_day_trade_strategy as switch


def test_held_lease_defers_guardian_and_releases_on_exit(tmp_path):
    path = tmp_path / "control.lock"
    with control_lease(owner="test-cutover", path=path) as owner:
        assert owner["acquired"]
        with control_lease(path=path) as guardian:
            assert not guardian["acquired"] and guardian["active"]
            assert guardian["holder"]["owner"] == "test-cutover"
        with pytest.raises(RuntimeError, match="another coordinator"):
            with control_lease(owner="other", path=path):
                pytest.fail("a second writer must not enter")
    # Leftover JSON is diagnostic only; a stale lease cannot disable recovery.
    assert json.loads(path.read_text())["owner"] == "test-cutover"
    with control_lease(path=path) as recovered:
        assert recovered["acquired"] and not recovered["active"]


def test_lease_is_released_if_coordinator_crashes(tmp_path):
    path = tmp_path / "control.lock"
    code = ("import time; from pathlib import Path; "
            "from stockagent.live.day_trade_maintenance import control_lease\n"
            f"with control_lease(owner='crashing-cutover', path=Path({str(path)!r})):\n"
            " print('held', flush=True)\n time.sleep(30)\n")
    child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "held"
        with control_lease(path=path) as guardian:
            assert not guardian["acquired"]
        child.kill()
        child.wait(timeout=5)
        with control_lease(path=path) as guardian:
            assert guardian["acquired"]
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        child.stdout.close()


def test_coordinator_uses_canonical_late_drain_without_cli_reparse(tmp_path, monkeypatch):
    from scripts import promote_tw_day_trade_replay as promoter
    events = []
    def promote(*, argv, before_exchange):
        assert argv == ["--candidate-dir", "candidate"]
        events.append("validate")
        before_exchange()
        events.append("exchange")
        print('{"status":"promoted"}')
    monkeypatch.setattr(promoter, "main", promote)
    receipt = switch._promote_with_cutover(
        [sys.executable, "canonical-promoter", "--candidate-dir", "candidate"],
        before_exchange=lambda: events.append("drain"), stage="promote", run_dir=tmp_path)
    assert events == ["validate", "drain", "exchange"]
    assert receipt["returncode"] == 0
    assert json.loads((tmp_path / "promote.stdout.log").read_text())["status"] == "promoted"


def test_failed_validation_never_drains_writer(tmp_path, monkeypatch):
    from scripts import promote_tw_day_trade_replay as promoter
    def invalid(**kwargs):
        raise ValueError("invalid source")
    monkeypatch.setattr(promoter, "main", invalid)
    with pytest.raises(ValueError, match="invalid source"):
        switch._promote_with_cutover([sys.executable, "canonical-promoter"],
            before_exchange=lambda: pytest.fail("invalid input must not stop service"),
            stage="promote", run_dir=tmp_path)
    assert "invalid source" in (tmp_path / "promote.stderr.log").read_text()


def test_deployment_schedule_preserves_secrets_and_all_previous_markets(tmp_path):
    path = tmp_path / "bot.env"
    secret_line = "EXAMPLE_TOKEN=do-not-copy-this-value\n"
    path.write_text(secret_line+"STOCKAGENT_SCHEDULED_MARKETS=first,second\nOTHER=unchanged\n")
    path.chmod(0o600)
    receipt = switch._ensure_discord_deployment_schedule("fifth", run_dir=tmp_path, env_path=path)
    text = path.read_text()
    assert secret_line in text and "OTHER=unchanged\n" in text
    assert "STOCKAGENT_SCHEDULED_MARKETS=first,second,fifth\n" in text
    assert secret_line not in json.dumps(receipt)
    assert path.stat().st_mode & 0o777 == 0o600
    deadline = datetime.fromisoformat(receipt["warmup_deadline"])
    assert 0 < (deadline-datetime.now(ZoneInfo("Asia/Taipei"))).total_seconds() <= 600
    switch._ensure_discord_deployment_schedule("fifth", run_dir=tmp_path, env_path=path)
    assert path.read_text().count("first,second,fifth") == 1


@pytest.mark.parametrize("schedule", [None, "all", "*"])
def test_schedule_update_does_not_expand_an_existing_all_policy(tmp_path, schedule):
    path = tmp_path / "bot.env"
    original = "OTHER=same\n"+(f"STOCKAGENT_SCHEDULED_MARKETS={schedule}\n" if schedule else "")
    path.write_text(original)
    switch._ensure_discord_deployment_schedule("fifth", run_dir=tmp_path, env_path=path)
    assert path.read_text().startswith(original)


@pytest.mark.parametrize("operation", ["add_independent_mode", "extend_history"])
def test_consumer_recovery_never_replays_an_unpromoted_operation(tmp_path, operation):
    from types import SimpleNamespace
    plan = {"operation": operation, "replacement": {"stable_market_id": "fifth"},
            "history": {}, "active_markets": ["fifth"], "current_deployment": {"matches": True}}
    with pytest.raises(ValueError, match="already-promoted"):
        switch._recover_promoted_consumers(SimpleNamespace(), plan,
            {"status": "failed_retryable", "promotion_completed": False})

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from scripts import verify_discord_natural_warmup as verify

NOW = datetime(2026, 10, 14, 0, 6, tzinfo=UTC)

def controls(monkeypatch):
    bot = SimpleNamespace(_resolve_market=lambda _market: SimpleNamespace(timezone="Asia/Taipei"),
                          _startup_gpu_prepare_minutes=lambda _cfg: 495)
    monkeypatch.setattr(verify.native, "verification_cache", lambda: SimpleNamespace(discord_bot=bot))
    names = [f"mode-{number}" for number in range(5)]
    warm = {"status": "ready", "run_id": "current-native-owner", "model_cache_resident_ready": True,
            "ready_count": 5, "market_count": 5, "elapsed_seconds": 15.3,
            "started_at": "2026-10-14T08:05:04+08:00", "completed_at": "2026-10-14T08:05:19+08:00",
            "markets": [{"market": name, "status": "ready"} for name in names]}
    status = {"run_id": "current-native-owner", "startup_inference_warmup": warm}
    control = {"observed_at_utc": "2026-10-13T23:19:00+00:00",
               "status": {"run_id": "current-native-owner"}, "signals": [{"market": name} for name in names],
               "startup_preparation": [{"market": name, "protection_start_minutes": 485} for name in names]}
    return status, control


def test_all_current_modes_must_finish_before_prepare_without_claiming_opening(monkeypatch):
    status, control = controls(monkeypatch)
    value = verify.warm_proof(status, control, NOW)
    assert value["state"] == "accepted_native_natural_warmup"
    assert value["completed_before_preparation"] and len(value["ready_modes"]) == 5


@pytest.mark.parametrize("phase", ["pending", "running"])
def test_natural_wait_is_not_completion_or_failure(monkeypatch, phase):
    status, control = controls(monkeypatch)
    monkeypatch.setattr(verify.native, "verification_cache", lambda: pytest.fail("waiting must not import inference"))
    status["startup_inference_warmup"] = {"status": phase}
    assert verify.warm_proof(status, control, NOW) == {"state": "waiting_natural_warmup", "phase": phase}


@pytest.mark.parametrize("boundary", ["old_owner", "cold_flag", "duplicate", "missing", "failed", "early", "late", "naive", "negative_time", "old_day", "future"])
def test_historical_partial_cold_or_wrong_window_never_claims_ready(monkeypatch, boundary):
    status, control = controls(monkeypatch)
    warm = status["startup_inference_warmup"]
    if boundary == "old_owner":
        warm["run_id"] = "old native owner"
    elif boundary == "cold_flag":
        warm["model_cache_resident_ready"] = False
    elif boundary == "duplicate":
        warm["markets"][0] = warm["markets"][1]
    elif boundary == "missing":
        warm["markets"].pop()
    elif boundary == "failed":
        warm["markets"][0]["status"] = "failed"
    elif boundary == "early":
        warm["started_at"] = "2026-10-14T07:03:00+08:00"
    elif boundary == "late":
        warm["completed_at"] = "2026-10-14T08:15:00+08:00"
    elif boundary == "naive":
        warm["started_at"] = "2026-10-14T08:05:04"
    elif boundary == "old_day":
        warm["started_at"] = "2026-10-13T08:05:04+08:00"
        warm["completed_at"] = "2026-10-13T08:05:19+08:00"
    elif boundary == "future":
        warm["completed_at"] = "2026-10-14T08:06:30+08:00"
    else:
        warm["elapsed_seconds"] = -1
    with pytest.raises((ValueError, TypeError)):
        verify.warm_proof(status, control, NOW)

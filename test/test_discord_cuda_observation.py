"""Allocator sampling remains an advisory heartbeat field."""
import json

import pytest

from services.discord_bot import bot as module


def test_service_heartbeat_publishes_process_allocator_observation(monkeypatch, tmp_path):
    sample = {"status": "available", "device": 0, "allocated_bytes": 100,
              "reserved_bytes": 200, "model_cache_entries": 2}
    monkeypatch.setattr(module, "_market_configs", lambda: {})
    monkeypatch.setattr(module, "_scheduled_markets", lambda: [])
    monkeypatch.setattr(module, "_day_trade_state_dir", lambda: tmp_path)
    monkeypatch.setattr(module, "load_service_sync", lambda _: {})
    monkeypatch.setattr(module, "_discord_service_status_path", lambda: tmp_path / "status.json")
    monkeypatch.setattr(module, "_artifact_backfill_health_summary", lambda: {})
    monkeypatch.setattr(module, "_signal_now_job_health_summary", lambda: {})
    monkeypatch.setattr(module, "_load_startup_inference_warmup_status", lambda: {})
    monkeypatch.setattr(module, "_load_postclose_fast_cache_status", lambda: {})
    monkeypatch.setattr(module, "live_cuda_allocator_observation", lambda: sample)
    monkeypatch.setattr(module.bot, "is_ready", lambda: True)
    monkeypatch.setattr(type(module.bot), "latency", property(lambda _: 0.1234567))
    monkeypatch.setattr(module.bot, "_scheduled_input_jobs", {})
    monkeypatch.setattr(module.bot, "_opening_input_stages", {})
    monkeypatch.setattr(module.bot, "_scheduled_delivery_task", None)
    payload = module._write_discord_service_status()
    assert payload["core_health"] == "ready"
    assert payload["cuda_allocator"] == sample
    assert payload["discord_gateway"] == {
        "status": "available", "heartbeat_ack_rtt_ms": 123.457,
        "sample_contract": "last_library_ack_rtt_ack_timestamp_unavailable_v1",
    }
    assert json.loads((tmp_path / "status.json").read_text())["cuda_allocator"] == sample
    assert json.loads((tmp_path / "status.json").read_text())["discord_gateway"] == payload["discord_gateway"]


@pytest.mark.parametrize(
    ("connected", "latency", "status"),
    [(False, 0.1, "disconnected"), (True, None, "unavailable"),
     (True, float("inf"), "unavailable"), (True, float("nan"), "unavailable"),
     (True, -1.0, "unavailable"), (True, 1e308, "unavailable")],
)
def test_gateway_rtt_observation_never_invents_fresh_or_valid_latency(connected, latency, status):
    result = module._discord_gateway_heartbeat_observation(connected=connected, latency_seconds=latency)
    assert result["status"] == status
    assert result["heartbeat_ack_rtt_ms"] is None
    assert "ack_timestamp_unavailable" in result["sample_contract"]


@pytest.mark.parametrize("previous, observed, interval, excess", [
    (None, 100., None, None), (100., 101., 1000., 0.),
    (100., 144., 44000., 43000.), (100., 99., None, None),
    (float("nan"), 101., None, None), (100., float("inf"), None, None),
])
def test_heartbeat_callback_timing_distinguishes_missing_from_measured_spacing(previous, observed, interval, excess):
    result = module._observe_service_heartbeat(previous, observed)
    assert result["callback_interval_ms"] == interval
    assert result["excess_over_period_ms"] == excess
    assert result["cause"] == "undetermined"

from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from scripts import verify_completed_session_cache as observer


@pytest.mark.parametrize("boundary", ["source_lease", "loaded_code"])
def test_diagnostic_persists_admission_rejections_without_reading_or_computing(
    tmp_path, monkeypatch, boundary,
):
    bot = observer.discord_bot
    configs = {name: SimpleNamespace(day_trade_simulation_enabled=True, config_path=name)
               for name in ("first", "second")}
    monkeypatch.setattr(bot, "_artifact_maintenance_markets", lambda: list(configs))
    monkeypatch.setattr(bot, "_resolve_market", configs.__getitem__)
    monkeypatch.setattr(bot, "_effective_market_config", lambda cfg: cfg)
    monkeypatch.setattr(observer, "load_config", lambda path: SimpleNamespace(
        data=SimpleNamespace(parquet_root=tmp_path)))
    monkeypatch.setattr(bot, "_ensure_signal_ready", lambda cfg: pytest.fail("blocked source read"))
    error = (observer.TWPublicSourceUpdatePending(tmp_path, tmp_path / "source.lock")
             if boundary == "source_lease" else observer.forward_cache.LoadedImplementationChanged())

    @contextmanager
    def reject(root):
        raise error
        yield  # pragma: no cover - expose the context-manager interface

    monkeypatch.setattr(observer, "live_tw_public_source_read_guard", reject)
    result = observer.verify(diagnose=True)
    assert result["state"] == "rejected" and result["market_count"] == 0
    assert result["requested_market_count"] == 2
    assert [row["market"] for row in result["errors"]] == list(configs)
    assert all(row["admission"] == error.details for row in result["errors"])
    assert result["model_forward_calls"] == result["provider_calls"] == 0
    with pytest.raises(type(error)):
        observer.verify()


def test_diagnostic_records_identity_mismatch_without_compute_or_false_acceptance(tmp_path, monkeypatch):
    configs = {name: SimpleNamespace(market=name, day_trade_simulation_enabled=True,
                                    config_path="fixture.yaml", day_trade_rule_data_dir=None)
               for name in ("good", "stale")}
    weights = tmp_path / "weights.parquet"
    weights.write_bytes(b"fixture")
    bot = observer.discord_bot
    monkeypatch.setattr(bot, "_artifact_maintenance_markets", lambda: list(configs))
    monkeypatch.setattr(bot, "_resolve_market", configs.__getitem__)
    monkeypatch.setattr(bot, "_effective_market_config", lambda cfg: cfg)
    monkeypatch.setattr(observer, "load_config", lambda path: SimpleNamespace(data=SimpleNamespace(parquet_root=tmp_path)))
    monkeypatch.setattr(bot, "_ensure_signal_ready", lambda cfg: SimpleNamespace(
        market_open=False, data=SimpleNamespace(fresh=True)))
    monkeypatch.setattr(bot, "_completed_session_receipt_ready", lambda status: True)
    monkeypatch.setattr(bot, "_latest_market_signal", lambda cfg: (
        tmp_path / "summary.json", {"panel_date": "2026-10-14", "signal_id": cfg.market,
                                   "live_latency": {"completed_forward_cache": {
                                       "contract": observer.CONTRACT, "key": "0" * 64}}}))
    monkeypatch.setattr(bot, "_can_reuse_latest_signal_now", lambda cfg, *a, **k: (
        cfg.market == "good", "cached_latest_official_close" if cfg.market == "good" else
        "completed_source_generation_changed"))
    monkeypatch.setattr(bot, "_summary_artifact_path", lambda *a: weights)
    calls = []

    def consume(cfg):
        calls.append(cfg.market)
        return None

    monkeypatch.setattr(bot, "_run_completed_session_signal_cache_sync", consume)
    monkeypatch.setattr(observer, "identity_diagnostic", lambda *a: {"configuration_matches": True})
    result = observer.verify(diagnose=True)
    assert result["state"] == "rejected"
    assert result["market_count"] == 1 and result["requested_market_count"] == 2
    assert [row["market"] for row in result["errors"]] == ["stale"]
    assert "completed_source_generation_changed" in result["errors"][0]["error"]
    assert calls == ["good"]
    assert result["cuda_initialized"] is False
    with pytest.raises(RuntimeError, match="artifact not reusable"):
        observer.verify()


@pytest.mark.parametrize("receipt_state", ("matching", "different_code", "invalid_key", "oversized", "foreign_contract"))
def test_diagnostic_uses_canonical_member_enumeration_and_bounded_receipt(
    tmp_path, monkeypatch, receipt_state,
):
    cache = observer.forward_cache
    members = [["canonical-member", [1, 2, 3, 4, 5]]]
    components = {"source": "source", "configuration": "config", "code": [{"sha256": "current"}],
                  "torch": "test", "numpy": "test"}

    def canonical(*args, **kwargs):
        cache.identity_digest({"contract": cache.CONTRACT, "members": members})
        cache.identity_digest(components)
        return "current-composite"

    monkeypatch.setattr(observer.discord_bot, "completed_panel_source_fingerprint", canonical)
    monkeypatch.setenv("STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT", str(tmp_path))
    key = "0" * 64 if receipt_state != "invalid_key" else "../private"
    summary = {"completed_panel_source_fingerprint": "old-composite",
               "live_latency": {"completed_forward_cache": {"key": key}}}
    receipt = {"contract": observer.CONTRACT, "key": key,
               "identity": {"resolved_config": "config", "code": components["code"],
                            "input": {"sha256": "whole-input"}, "mask": {"sha256": "whole-mask"}}}
    if receipt_state == "different_code":
        receipt["identity"]["code"] = [{"sha256": "old"}]
    elif receipt_state == "foreign_contract":
        receipt["contract"] = "old-contract"
    if receipt_state != "invalid_key":
        (tmp_path / f"{key}.receipt.json").write_bytes(
            b" " * (1024**2 + 1) if receipt_state == "oversized" else json.dumps(receipt).encode())
    cfg = SimpleNamespace(config_path="fixture.yaml", day_trade_rule_data_dir=None)
    observed = observer.identity_diagnostic(cfg, summary)
    assert observed["source_members"] == members
    assert observed["current_identity_components"] == components
    assert observed["current_composite_identity"] == "current-composite"
    assert observed["saved_composite_identity"] == "old-composite"
    if receipt_state in {"matching", "different_code"}:
        assert observed["resolved_config_matches"] is True
        assert observed["implementation_matches"] is (receipt_state == "matching")
        assert observed["forward_input_identity"] == {"sha256": "whole-input"}
    else:
        assert observed.get("forward_receipt_error")
        assert "resolved_config_matches" not in observed

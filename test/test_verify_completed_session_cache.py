from types import SimpleNamespace
import json

import pytest

from scripts import verify_completed_session_cache as verifier


@pytest.fixture
def reusable(tmp_path, monkeypatch):
    # This fixture models a separate cold observer, including in a full suite
    # where earlier model tests legitimately initialized this pytest process.
    monkeypatch.setattr(verifier.torch.cuda, "is_initialized", lambda: False)
    weights = tmp_path / "weights.csv"
    weights.write_text("symbol,weight\n2330,0.1\n")
    cfg = SimpleNamespace(market="tw", day_trade_simulation_enabled=True,
                          config_path=tmp_path / "config.yaml")
    status = SimpleNamespace(market_open=False, data=SimpleNamespace(fresh=True))
    summary = {"panel_date": "2026-10-13 13:30:00", "signal_id": "accepted",
               "completed_panel_source_fingerprint": "current",
               "live_latency": {"completed_forward_cache":
                                {"contract": verifier.CONTRACT, "key": "exact-input"}}}
    bot = verifier.discord_bot
    monkeypatch.setattr(bot, "_artifact_maintenance_markets", lambda: ["tw"])
    monkeypatch.setattr(bot, "_resolve_market", lambda market: cfg)
    monkeypatch.setattr(bot, "_effective_market_config", lambda value: value)
    monkeypatch.setattr(bot, "_ensure_signal_ready", lambda value: status)
    monkeypatch.setattr(bot, "_completed_session_receipt_ready", lambda value: True)
    monkeypatch.setattr(bot, "_latest_market_signal", lambda value: (tmp_path / "summary.json", summary))
    monkeypatch.setattr(bot, "_summary_artifact_path", lambda *args: weights)
    monkeypatch.setattr(bot, "_can_reuse_latest_signal_now", lambda *args, **kw: (True, "verified"))
    monkeypatch.setattr(verifier, "load_config", lambda path:
                        SimpleNamespace(data=SimpleNamespace(parquet_root=tmp_path / "stocks")))
    calls = []
    monkeypatch.setattr(bot, "_run_completed_session_signal_cache_sync",
                        lambda value: calls.append(value.market))
    return cfg, status, summary, weights, calls


def test_reuse_verifier_invokes_canonical_consumer_and_reports_no_cuda(reusable):
    *_, calls = reusable
    result = verifier.verify()
    assert result["market_count"] == 1 and calls == ["tw"]
    assert result["cuda_initialized"] is False and result["model_forward_calls"] == 0


@pytest.mark.parametrize("invalid", ["open", "stale", "preview", "old_contract", "missing_weights"])
def test_reuse_verifier_rejects_incomplete_or_unsafe_artifacts(reusable, invalid):
    _, status, summary, weights, calls = reusable
    if invalid == "open":
        status.market_open = True
    elif invalid == "stale":
        status.data.fresh = False
    elif invalid == "preview":
        summary["postclose_fast_preview"] = True
    elif invalid == "old_contract":
        summary["live_latency"]["completed_forward_cache"]["contract"] = "old"
    else:
        weights.unlink()
    with pytest.raises(RuntimeError):
        verifier.verify()
    assert not calls


def test_reuse_verifier_actually_forbids_model_work(reusable, monkeypatch):
    monkeypatch.setattr(verifier.discord_bot, "_run_completed_session_signal_cache_sync",
                        lambda cfg: verifier.discord_bot.generate_live_signal())
    with pytest.raises(AssertionError, match="model or CUDA"):
        verifier.verify()


def test_reuse_verifier_rejects_a_process_that_already_initialized_cuda(monkeypatch):
    monkeypatch.setattr(verifier.torch.cuda, "is_initialized", lambda: True)
    with pytest.raises(RuntimeError, match="CUDA-cold"):
        verifier.verify()


@pytest.mark.parametrize("bad", [None, "hash", "key", "cuda", "gpu_bytes"])
def test_benchmark_acceptance_requires_exact_output_and_formal_key(tmp_path, bad):
    candidate = {"market": "tw", "output_sha256": "expected", "cuda_initialized": False,
                 "network_requests": 0, "production_writes": False, "warm_median_ms": 1.,
                 "samples": [{"rss_peak_bytes": 10, "gpu_peak_allocated_bytes": 0,
                              "gpu_peak_reserved_bytes": 0,
                              "stages": {"completed_forward_cache":
                                         {"key": "exact", "contract": verifier.CONTRACT,
                                          "state": "disk_hit", "gpu_forward_calls": 0}}}]}
    if bad == "hash":
        candidate["output_sha256"] = "wrong"
    elif bad == "key":
        candidate["samples"][0]["stages"]["completed_forward_cache"]["key"] = "other"
    elif bad == "cuda":
        candidate["cuda_initialized"] = True
    elif bad == "gpu_bytes":
        candidate["samples"][0]["gpu_peak_reserved_bytes"] = 1
    (tmp_path / "postclose-accepted-fresh-tw.json").write_text(json.dumps(candidate))
    (tmp_path / "postclose-control-tw.json").write_text(json.dumps({"output_sha256": "expected"}))
    result = {"markets": [{"market": "tw", "cache_key": "exact"}]}
    if bad is not None:
        with pytest.raises(RuntimeError, match="mismatch|another key"):
            verifier.matched_benchmarks(result, tmp_path)
    else:
        assert verifier.matched_benchmarks(result, tmp_path)[0]["exact_output_parity"] is True


def test_new_generation_uses_separate_unchanged_independent_controls(tmp_path):
    fresh, controls = tmp_path / "fresh", tmp_path / "controls"
    fresh.mkdir()
    controls.mkdir()
    original = b'{"output_sha256":"expected"}\n'
    (controls / "postclose-control-tw.json").write_bytes(original)
    candidate = {"market": "tw", "output_sha256": "expected", "cuda_initialized": False,
                 "network_requests": 0, "production_writes": False, "warm_median_ms": 1.,
                 "samples": [{"rss_peak_bytes": 10, "gpu_peak_allocated_bytes": 0,
                              "gpu_peak_reserved_bytes": 0, "stages": {"completed_forward_cache":
                                  {"key": "new-generation", "contract": verifier.CONTRACT,
                                   "state": "disk_hit", "gpu_forward_calls": 0}}}]}
    (fresh / "postclose-accepted-fresh-tw.json").write_text(json.dumps(candidate))
    result = {"markets": [{"market": "tw", "cache_key": "new-generation"}]}
    assert verifier.matched_benchmarks(result, fresh, controls)[0]["control_path"] == str(controls / "postclose-control-tw.json")
    assert (controls / "postclose-control-tw.json").read_bytes() == original
    assert not (fresh / "postclose-control-tw.json").exists()

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.live import completed_inference_cache as cache
from stockagent.live import signal_engine
from stockagent.config import DataConfig, load_config


def _bits(tensor):
    return tensor.contiguous().view(torch.uint8).numpy().tobytes()


@pytest.mark.parametrize("dtype", [torch.float32, torch.float64, torch.bfloat16])
def test_lossless_outputs_are_owned_cpu_tensors_and_survive_reload(tmp_path, dtype):
    tensor = torch.tensor([0.0, -0.0, float("nan"), float("inf"), -2.5], dtype=dtype)
    original = {"weights": tensor, "aux": (tensor[1:3], None, [True, "raw"])}
    value, first = cache.cached_forward(tmp_path, {"input": "a"}, lambda: original)
    saved = _bits(value["weights"])
    tensor.fill_(1)
    loaded, second = cache.cached_forward(tmp_path, {"input": "a"},
        lambda: pytest.fail("cache hit must not compute"))
    assert _bits(loaded["weights"]) == saved
    assert _bits(loaded["aux"][0]) == _bits(value["aux"][0])
    assert loaded["weights"].dtype == dtype
    assert loaded["weights"].device.type == "cpu"
    assert first["state"] == "computed" and second["state"] == "disk_hit"
    assert second["gpu_forward_calls"] == 0
    assert all(path.stat().st_mode & 0o077 == 0 for path in tmp_path.iterdir())
    loaded["weights"].fill_(0)
    reloaded, _ = cache.cached_forward(tmp_path, {"input": "a"}, lambda: pytest.fail("compute"))
    assert _bits(reloaded["weights"]) == saved


def test_safe_none_result_and_tuple_identity_can_be_reused(tmp_path):
    cache.cached_forward(tmp_path, {"shape": (1, 2)}, lambda: None)
    value, receipt = cache.cached_forward(tmp_path, {"shape": (1, 2)}, lambda: pytest.fail("compute"))
    assert value is None and receipt["state"] == "disk_hit"


def test_incomplete_payload_receipt_commit_is_repaired(tmp_path):
    identity = {"input": "crash"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([1.]))
    (tmp_path / f"{cache.identity_digest(identity)}.receipt.json").unlink()
    value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([2.]))
    assert receipt["repaired_invalid_cache"] and value.tolist() == [2.]


@pytest.mark.parametrize("damage", [b"[]", b'{"payload_bytes":0}', b""])
def test_corrupt_receipt_is_repaired_not_accepted(tmp_path, damage):
    identity = {"input": "a"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3.]))
    (tmp_path / f"{cache.identity_digest(identity)}.receipt.json").write_bytes(damage)
    value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([4.]))
    assert receipt["repaired_invalid_cache"] and value.tolist() == [4.]


def test_receipt_symlink_does_not_disclose_or_overwrite_target(tmp_path):
    identity = {"input": "a"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3.]))
    receipt = tmp_path / f"{cache.identity_digest(identity)}.receipt.json"
    receipt.unlink()
    target = tmp_path / "unrelated"
    target.write_text("preserve")
    receipt.symlink_to(target)
    _, result = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([4.]))
    assert result["repaired_invalid_cache"] and target.read_text() == "preserve"


def test_shared_writable_mapping_is_rejected(tmp_path, monkeypatch):
    import mmap

    identity = {"input": "a"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3.]))
    monkeypatch.setattr(torch, "load", lambda *a, **kw: pytest.fail("unsafe map"))
    with torch.serialization.set_default_mmap_options(mmap.MAP_SHARED):
        value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([4.]))
    assert receipt["repaired_invalid_cache"] and value.tolist() == [4.]


def test_fresh_process_hit_never_initializes_or_queries_cuda(tmp_path):
    cache.cached_forward(tmp_path, {"input": "persisted"}, lambda: torch.tensor([2.0]))
    program = """
import sys
from pathlib import Path
import torch
from stockagent.live.completed_inference_cache import cached_forward
def forbidden(*a, **kw):
    raise AssertionError('GPU or model compute attempted on a cache hit')
torch.cuda.is_available = forbidden
torch.cuda.init = forbidden
torch.cuda.synchronize = forbidden
torch.cuda.empty_cache = forbidden
assert not torch.cuda.is_initialized()
value, receipt = cached_forward(Path(sys.argv[1]), {'input': 'persisted'}, forbidden)
assert value.tolist() == [2.0] and receipt['gpu_forward_calls'] == 0
assert not torch.cuda.is_initialized()
"""
    result = subprocess.run([sys.executable, "-c", program, str(tmp_path)],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("damage", [b"", b"[]\ninvalid", b'{"key":"wrong"}\ninvalid'])
def test_corrupt_cache_is_rebuilt_not_returned(tmp_path, damage):
    identity = {"input": "a"}
    path = tmp_path / f"{cache.identity_digest(identity)}.forward"
    path.write_bytes(damage)
    value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3]))
    assert value.tolist() == [3]
    assert receipt["repaired_invalid_cache"] is True
    _, receipt = cache.cached_forward(tmp_path, identity, lambda: pytest.fail("not rebuilt"))
    assert receipt["state"] == "disk_hit"


def test_payload_tampering_and_symlink_cannot_be_loaded(tmp_path):
    identity = {"input": "a"}
    cache.cached_forward(tmp_path, identity, lambda: torch.tensor([3]))
    path = tmp_path / f"{cache.identity_digest(identity)}.forward"
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    _, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([4]))
    assert receipt["repaired_invalid_cache"]
    target = tmp_path / "unrelated"
    target.write_text("preserve this")
    path.unlink()
    path.symlink_to(target)
    value, receipt = cache.cached_forward(tmp_path, identity, lambda: torch.tensor([5]))
    assert value.tolist() == [5] and receipt["repaired_invalid_cache"]
    assert target.read_text() == "preserve this" and not path.is_symlink()


def test_concurrent_same_identity_computes_exactly_once(tmp_path):
    barrier = threading.Barrier(6)
    calls = []

    def compute():
        calls.append(True)
        time.sleep(0.08)
        return torch.tensor([7.0])

    def request():
        barrier.wait(timeout=5)
        return cache.cached_forward(tmp_path, {"input": "same"}, compute)

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: request(), range(6)))
    assert calls == [True]
    assert sum(receipt["state"] == "computed" for _, receipt in results) == 1
    assert all(value.tolist() == [7.0] for value, _ in results)


def test_failed_compute_or_changed_identity_is_not_published(tmp_path):
    def changed():
        raise RuntimeError("changed before commit")

    with pytest.raises(RuntimeError, match="changed"):
        cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.tensor([1]),
                             validate_before_commit=changed)
    assert not list(tmp_path.glob("*.forward"))
    cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.tensor([2]))
    # Revalidate hits as well as newly computed outputs.
    with pytest.raises(RuntimeError, match="changed"):
        cache.cached_forward(tmp_path, {"input": "a"}, lambda: pytest.fail("compute"),
                             validate_before_commit=changed)


def test_bounded_wait_and_payload_reject_without_deadlock(tmp_path, monkeypatch):
    path = tmp_path / "same.lock"
    with cache._singleflight(path):
        with pytest.raises(TimeoutError):
            with cache._singleflight(path, timeout_seconds=0.01):
                pytest.fail("lock was not exclusive")
    monkeypatch.setattr(cache, "MAX_PAYLOAD_BYTES", 32)
    with pytest.raises(ValueError, match="bounded"):
        cache.cached_forward(tmp_path, {"input": "a"}, lambda: torch.ones(100))
    assert not list(tmp_path.glob("*.forward"))


def test_input_identities_preserve_shape_dtype_order_and_empty_arrays():
    array = np.array([[0.0, -0.0], [1.0, 2.0]], dtype=np.float32)
    assert cache.array_identity(array) != cache.array_identity(array[:, ::-1])
    assert cache.array_identity(array) != cache.array_identity(array.astype(np.float64))
    assert cache.array_identity(array) != cache.array_identity(array.reshape(-1))
    assert cache.array_identity(np.empty((0, 3), dtype=np.float32))["shape"] == [0, 3]
    with pytest.raises(ValueError, match="Python objects"):
        cache.array_identity(np.array([object()], dtype=object))


def test_same_day_in_place_source_repair_invalidates_generation(tmp_path):
    root = tmp_path / "data" / "stock"
    root.mkdir(parents=True)
    member = root / "2330_features.parquet"
    member.write_bytes(b"before")
    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index"))
    first = cache.source_fingerprint(config)
    before = member.stat()
    member.write_bytes(b"repair")
    os.utime(member, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert cache.source_fingerprint(config) != first
    member.rename(root / "2317_features.parquet")
    assert cache.source_fingerprint(config) != first


def test_source_identity_ignores_unconsumed_provider_tables_and_directory_churn(tmp_path):
    root = tmp_path / "data" / "stocks"
    root.mkdir(parents=True)
    (root / "2330_features.parquet").write_bytes(b"published")
    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index"))
    fingerprint = lambda: cache.source_fingerprint(config, rule_data_dir=root.parent)
    before = fingerprint()
    raw = root.parent / "tpex_api_mopsfin_t187ap13_o.parquet"
    raw.write_bytes(b"new unconsumed provider snapshot")
    (root / "status.json").write_text("heartbeat")
    assert fingerprint() == before
    raw.write_bytes(b"next snapshot")
    assert fingerprint() == before
    (root / "2317_features.parquet").write_bytes(b"new consumed member")
    assert fingerprint() != before


def test_fallback_eligibility_cache_identity_detects_repair_with_restored_mtime(tmp_path, monkeypatch):
    root = tmp_path / "stocks"
    root.mkdir()
    paths = [tmp_path / "twse_day_trade_eligibility.parquet",
             tmp_path / "tpex_day_trade_eligibility.parquet", root / "symbols.csv"]
    for path in paths:
        path.write_bytes(b"before")
    panel = SimpleNamespace(dates=np.array([np.datetime64("2020-01-01")]),
                            num_dates=1, symbols=["2330"], day_trade_eligible_mask=None)
    identities = []

    def rules(*args):
        identities.append(args[-1])
        return np.ones(1, dtype=bool), np.ones(1, dtype=bool), {"source": "checked"}

    monkeypatch.setattr(signal_engine, "_cached_session_day_trade_rules", rules)
    kwargs = dict(session_date="2020-01-02", parquet_root=root, rule_data_dir=tmp_path)
    signal_engine._day_trade_model_eligibility(panel, **kwargs)
    before = paths[0].stat()
    paths[0].write_bytes(b"repair")
    os.utime(paths[0], ns=(before.st_atime_ns, before.st_mtime_ns))
    signal_engine._day_trade_model_eligibility(panel, **kwargs)
    assert identities[0] != identities[1]


@pytest.mark.parametrize("kind", ["feature", "receipt", "eligibility", "action", "entitlement"])
def test_source_identity_tracks_consumed_external_rules_and_receipts(tmp_path, kind):
    root = tmp_path / "data" / "stocks"
    root.mkdir(parents=True)
    external = root.parent / "features" / "public.parquet"
    external.parent.mkdir()
    external.write_bytes(b"features")
    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index",
        use_tw_public_features=True, tw_public_feature_path=str(external)))
    fingerprint = lambda: cache.source_fingerprint(config, rule_data_dir=root.parent)
    before = fingerprint()
    member = {"feature": external, "receipt": external.with_suffix(".summary.json"),
              "eligibility": root.parent / "twse_day_trade_eligibility.parquet",
              "action": root.parent / "tw_corporate_action_reference.parquet",
              "entitlement": root.parent / "tw_corporate_action_entitlements.summary.json"}[kind]
    member.write_bytes(b"repaired published input")
    assert fingerprint() != before


def test_factorized_identity_uses_verified_content_without_dense_materialization():
    class Window:
        _stockagent_factorized_live_window = True
        features = SimpleNamespace(content_fingerprint={"sha256": "verified"})
        start, stop, shape = 0, 3, (3, 2, 4)
        overlay = None

        def __array__(self, *args):
            pytest.fail("must not materialize the full factorized panel")

    window = Window()
    first = cache.window_identity(window)
    window.overlay = (2, np.ones(2, dtype=np.float32))
    assert cache.window_identity(window) != first


def _toy_forward(tmp_path, monkeypatch):
    config_path, checkpoint = tmp_path / "config.yaml", tmp_path / "checkpoint.pt"
    config_path.write_text("runtime: stable")
    checkpoint.write_bytes(b"model-identity")
    monkeypatch.setenv("STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT", str(tmp_path / "cache"))
    calls = []
    monkeypatch.setattr(signal_engine, "resolve_device", lambda _: torch.device("cpu"))
    monkeypatch.setattr(signal_engine, "_cached_live_model", lambda **_: (torch.nn.Identity(), False))

    def forward(model, x, mask, **kwargs):
        calls.append(mask.clone())
        weights = mask.to(torch.float32) / 10
        return weights, {"raw_scores": weights.clone(), "aux": torch.tensor([1.0], dtype=torch.bfloat16)}

    monkeypatch.setattr(signal_engine, "call_model", forward)
    config = load_config("configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_live_v1.yaml")
    config.training.lookback = 3
    config.environment.device = "cpu"
    kwargs = dict(config=config,
        config_path=config_path, checkpoint=checkpoint,
        checkpoint_key=signal_engine._checkpoint_cache_key(checkpoint), state_dict={},
        panel=SimpleNamespace(symbols=["2330", "2317"], feature_names=["a", "b", "c", "d"],
            dates=["2026-10-09", "2026-10-12", "2026-10-13"], num_symbols=2),
        x_np=np.ones((3, 2, 4), dtype=np.float32), mask_np=np.array([True, False]),
        runtime_device=torch.device("cpu"), amp_dtype=None, non_blocking=False,
        include_unconstrained=False, completed_cache=True, panel_idx=2,
        source_fingerprint="accepted-close")
    return kwargs, calls


def test_canonical_forward_hit_skips_model_and_optional_raw_has_separate_mask(tmp_path, monkeypatch):
    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    first, _, _, receipt = signal_engine._live_forward_outputs(**kwargs)
    assert receipt["state"] == "computed" and len(calls) == 1
    result, raw, _, receipt = signal_engine._live_forward_outputs(
        **{**kwargs, "include_unconstrained": True})
    assert _bits(result[0]) == _bits(first[0]) and len(calls) == 2
    assert raw[0].tolist() == [[pytest.approx(0.1), pytest.approx(0.1)]]
    assert receipt["state"] == "disk_hit" and receipt["unconstrained"]["state"] == "computed"
    monkeypatch.setattr(signal_engine, "resolve_device", lambda _: pytest.fail("GPU readiness queried"))
    monkeypatch.setattr(signal_engine, "_cached_live_model", lambda **_: pytest.fail("model loaded"))
    _, _, _, receipt = signal_engine._live_forward_outputs(**{**kwargs, "include_unconstrained": True})
    assert receipt["state"] == receipt["unconstrained"]["state"] == "disk_hit"


@pytest.mark.parametrize("change", ["mask", "input", "config", "inherited_config"])
def test_changed_forward_identity_recomputes(tmp_path, monkeypatch, change):
    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    signal_engine._live_forward_outputs(**kwargs)
    if change == "mask":
        kwargs["mask_np"] = np.ones(2, dtype=bool)
    elif change == "input":
        kwargs["x_np"][0, 0, 0] = 2
    elif change == "config":
        kwargs["config_path"].write_text("runtime: updated")
    else:
        kwargs["config"].training.seed += 1
    _, _, _, receipt = signal_engine._live_forward_outputs(**kwargs)
    assert receipt["state"] == "computed" and len(calls) == 2


def test_source_metadata_revision_rebuilds_summary_but_does_not_duplicate_identical_forward(tmp_path, monkeypatch):
    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    signal_engine._live_forward_outputs(**kwargs)
    kwargs["source_fingerprint"] = "newly-accepted-source"
    _, _, _, receipt = signal_engine._live_forward_outputs(**kwargs)
    assert receipt["state"] == "disk_hit" and len(calls) == 1
    assert receipt["accepted_source_generation"] == "newly-accepted-source"


def test_deployment_racing_forward_cannot_publish_old_result_under_new_identity(tmp_path, monkeypatch):
    kwargs, _ = _toy_forward(tmp_path, monkeypatch)
    original = signal_engine.call_model

    def replace(*args, **values):
        result = original(*args, **values)
        kwargs["config_path"].write_text("changed while computing")
        return result

    monkeypatch.setattr(signal_engine, "call_model", replace)
    with pytest.raises(RuntimeError, match="identity changed"):
        signal_engine._live_forward_outputs(**kwargs)
    assert not list((tmp_path / "cache").glob("*.forward"))


def test_live_forward_bypasses_persistent_cache(tmp_path, monkeypatch):
    kwargs, calls = _toy_forward(tmp_path, monkeypatch)
    kwargs["completed_cache"] = False
    for _ in range(2):
        _, _, _, receipt = signal_engine._live_forward_outputs(**kwargs)
        assert receipt is None
    assert len(calls) == 2 and not (tmp_path / "cache").exists()


def test_model_cache_key_includes_resolved_inherited_configuration(tmp_path, monkeypatch):
    kwargs, _ = _toy_forward(tmp_path, monkeypatch)
    args = {"checkpoint_key": kwargs["checkpoint_key"], "runtime_device": kwargs["runtime_device"],
            "panel": kwargs["panel"], "lookback": 3, "config": kwargs["config"]}
    first = signal_engine._model_cache_key(**args)
    kwargs["config"].training.financial_transformer.causal_feature_rms_normalization = False
    assert signal_engine._model_cache_key(**args) != first


def test_forward_inventory_counts_full_tensor_outputs_without_value_reads():
    from scripts.benchmark_live_inference import forward_tensor_inventory

    output = (torch.empty((2, 3), device="meta"), None,
              {"bf16": torch.empty((7,), dtype=torch.bfloat16, device="meta")})
    rows = forward_tensor_inventory(output)
    assert sum(row["bytes"] for row in rows) == 24 + 14
    assert [row["path"] for row in rows] == ["output[0]", "output[2].bf16"]

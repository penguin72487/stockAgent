import numpy as np
import pytest
import torch

from stockagent.data.factorized_panel import (
    FactorizedDateView, FactorizedLiveWindow, FactorizedPanelSlab, attach_factorized_features,
)
from stockagent.models.financial_transformer import FinancialTransformerModel
from stockagent.training.runtime import call_model
from test_factorized_panel import example


def make_model():
    model = FinancialTransformerModel(lookback=4, num_features=10, num_symbols=3,
        d_model=8, attention_mode="market_token", temporal_layers=1,
        temporal_heads=2, cross_heads=2, num_market_tokens=1, market_layers=1,
        head_hidden_dim=8, head_layers=1, dropout=0., input_dropout=0.,
        candle_dropout=0., causal_feature_rms_normalization=True,
        sanitize_inputs=False).eval()
    model.candle_encoder.set_causal_feature_rms_normalizer(
        torch.arange(1, 11, dtype=torch.float32), torch.arange(10) % 3 != 0)
    return model


def assert_tree_bits(a, b):
    if isinstance(a, torch.Tensor):
        assert a.dtype == b.dtype and a.shape == b.shape
        assert torch.equal(a.contiguous().view(torch.uint8), b.contiguous().view(torch.uint8))
    elif isinstance(a, dict):
        assert a.keys() == b.keys()
        for key in a:
            assert_tree_bits(a[key], b[key])
    elif isinstance(a, (tuple, list)):
        assert len(a) == len(b)
        for x, y in zip(a, b, strict=True):
            assert_tree_bits(x, y)
    else:
        assert a == b


@pytest.mark.parametrize(("device", "direct_destination", "prepared_host_packets"),
                         [("cpu", False, False), ("cuda", False, False),
                          ("cuda", True, False), ("cuda", False, True)])
@pytest.mark.parametrize("overlay", [False, True])
def test_live_transport_complete_outputs_preserve_bits_and_source(tmp_path, monkeypatch, device, direct_destination, prepared_host_packets, overlay):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    panel, path, dense = example(tmp_path)
    panel.features[4, 0, 0] = dense[4, 0, 0] = -0.
    panel.features[5, 1, 1] = dense[5, 1, 1] = float("nan")
    attach_factorized_features(panel, path, host_cache_bytes=4096)
    if prepared_host_packets:
        panel.features.base.setflags(write=False)
        panel.features._live_prepared_packet_owner = True
    view = FactorizedDateView(panel.features, [1, 3, 4, 5, 6])
    window = FactorizedLiveWindow(view, 1, 5)
    expected = np.nan_to_num(dense[[3, 4, 5, 6]], nan=0., posinf=0., neginf=0.)
    original = view[1:5].copy()
    if overlay:
        values = np.array([-.02, -0., .025], dtype=np.float32)
        window = window.with_last_feature(1, values)
        expected[-1, :, 1] = values
        values[:] = 1.  # The request owns its copied quote overlay.
    target = torch.device(device)
    with torch.inference_mode():
        actual = window.materialize(target, direct_destination=direct_destination,
                                    prepared_host_packets=prepared_host_packets).cpu().numpy()[0]
    assert actual.tobytes() == expected.tobytes()
    assert window[-1].tobytes() == expected[-1].tobytes()
    with pytest.raises(IndexError):
        window[0]
    model = make_model().to(target)
    mask = torch.tensor([[True, False, True]], device=target)
    if direct_destination or prepared_host_packets:
        materialize = FactorizedLiveWindow.materialize
        monkeypatch.setattr(FactorizedLiveWindow, "materialize",
                            lambda self, target: materialize(self, target,
                                direct_destination=direct_destination, prepared_host_packets=prepared_host_packets))
    with torch.inference_mode(), torch.autocast(device, dtype=torch.bfloat16,
                                              enabled=device == "cuda"):
        control = call_model(model, torch.from_numpy(expected).unsqueeze(0).to(target),
                             mask, return_aux=True)
        candidate = call_model(model, window, mask, return_aux=True)
        assert_tree_bits(control, candidate)
        assert_tree_bits(candidate, call_model(model, window, mask, return_aux=True))
        lean = call_model(model, window, mask, return_aux=True, materialize_candle_aux=False)
        assert_tree_bits(control[:2], lean[:2])
        assert lean[2].keys() == control[2].keys()
        for key, value in lean[2].items():
            if key in {"candle_tokens", "candle_token_weights", "candle_embedding"}:
                assert value is None
            else:
                assert_tree_bits(control[2][key], value)
    assert view[1:5].tobytes() == original.tobytes()
    if prepared_host_packets:
        assert panel.features._cache.prepared_hits > 0


def test_prepared_packets_preserve_order_overlay_and_one_existing_byte_budget(tmp_path):
    if not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path, host_cache_bytes=4096)
    source, target = panel.features, torch.device("cuda")
    window = FactorizedLiveWindow(FactorizedDateView(source, [1, 3, 4, 5, 6]), 1, 5)
    cache = source._cache
    with torch.inference_mode():
        mutable = window.materialize(target, prepared_host_packets=True).cpu().numpy()
        assert cache.prepared_hits == cache.prepared_misses == 0
        assert window.transport_stats["prepared_host_packets_requested"] is True
        assert window.transport_stats["prepared_host_packets_active"] is False
        source.base.setflags(write=False)
        source._live_prepared_packet_owner = True
        assert window.materialize(target).cpu().numpy().tobytes() == mutable.tobytes()
        assert window.transport_stats["prepared_host_packets_active"] is True
        misses, admissions = cache.prepared_misses, cache.prepared_admissions
        assert misses > 0 and admissions > 0
        assert window.materialize(target, prepared_host_packets=False).cpu().numpy().tobytes() == mutable.tobytes()
        assert window.transport_stats["prepared_host_packets_active"] is False
        assert cache.prepared_misses == misses
        overlay = window.with_last_feature(1, [-.02, -0., .025])
        expected = overlay.materialize(target).cpu().numpy()
        assert overlay.materialize(target, prepared_host_packets=True).cpu().numpy().tobytes() == expected.tobytes()
        assert cache.prepared_hits > 0 and cache.prepared_misses == misses
        assert window.materialize(target, prepared_host_packets=True).cpu().numpy().tobytes() == mutable.tobytes()
        for view in (FactorizedDateView(source, [6, 3, 4, 5]),
                     FactorizedDateView(source.subset_symbols(np.array([2, 0, 1])), [3, 4, 5, 6]),
                     FactorizedDateView(source.slice_dates(1, 7), [2, 3, 4, 5])):
            other = FactorizedLiveWindow(view, 0, 4)
            expected = other.materialize(target).cpu().numpy()
            assert other.materialize(target, prepared_host_packets=True).cpu().numpy().tobytes() == expected.tobytes()
        assert cache.prepared_misses > misses
        assert cache.bytes == sum(value.nbytes for value in cache.cache.values()) <= cache.budget_bytes
        old_hits = cache.prepared_hits
        cache.cache.clear()
        cache.bytes = 0
        cache.budget_bytes = 8
        assert window.materialize(target, prepared_host_packets=True).cpu().numpy().tobytes() == mutable.tobytes()
        assert cache.bytes <= cache.budget_bytes and cache.prepared_hits == old_hits


def test_uncacheable_host_packet_never_evicts_current_source_blocks(tmp_path):
    from stockagent.data.factorized_panel import _BlockCache
    cache = _BlockCache(tmp_path, [], 32)
    existing = np.arange(4, dtype=np.float32)
    assert cache._admit(0, existing)
    assert not cache._admit(("too-large",), np.arange(20, dtype=np.float32))
    assert cache.cache[0] is existing and cache.bytes == existing.nbytes
    import pickle
    assert pickle.loads(pickle.dumps(cache)).bytes == cache.bytes


def test_live_raw_device_packets_are_bounded_stream_safe_and_retirable(tmp_path, monkeypatch):
    if not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path, host_cache_bytes=4096)
    source = panel.features
    source.base.setflags(write=False)
    source._live_prepared_packet_owner = True
    cache, target = source._cache, torch.device("cuda")
    window = FactorizedLiveWindow(source, 2, 6)
    with torch.inference_mode():
        control = window.materialize(target, prepared_host_packets=False).cpu()
        first = window.materialize(target).cpu()
        assert_tree_bits(control, first)
        assert window.transport_stats["host_payload_bytes"] > 0
        assert 0 < cache._live_device_bytes <= min(cache.budget_bytes, 1024**3)
        alternate = torch.cuda.Stream()
        with torch.cuda.stream(alternate):
            assert_tree_bits(first, window.materialize(target).cpu())
        assert window.transport_stats["host_payload_bytes"] == 0
        assert window.transport_stats["live_device_packet_hits"] > 0
        altered = window.with_last_feature(1, [-.01, -0., .02])
        expected = altered.materialize(target, prepared_host_packets=False).cpu()
        assert_tree_bits(expected, altered.materialize(target).cpu())
        assert_tree_bits(first, window.materialize(target).cpu())
        # A new row order cannot reuse the old raw payload or its coordinates.
        other = FactorizedLiveWindow(FactorizedDateView(source, [5, 3, 4, 2]), 0, 4)
        assert_tree_bits(other.materialize(target, prepared_host_packets=False).cpu(),
                         other.materialize(target).cpu())
        assert other.transport_stats["host_payload_bytes"] > 0
        assert cache.retire_live_device_packets() is True
        assert not cache._live_device_packets and cache._live_device_bytes == 0
        assert_tree_bits(first, window.materialize(target).cpu())
        assert window.transport_stats["host_payload_bytes"] > 0
        assert cache._live_device_bytes == 0  # old in-flight owners stay retired


def test_live_raw_device_capacity_miss_keeps_the_complete_input(tmp_path, monkeypatch):
    if not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path, host_cache_bytes=4096)
    source = panel.features
    source.base.setflags(write=False)
    source._live_prepared_packet_owner = True
    window = FactorizedLiveWindow(source, 2, 6)
    with torch.inference_mode():
        control = window.materialize(torch.device("cuda"), prepared_host_packets=False).cpu()
        monkeypatch.setattr(torch.cuda, "mem_get_info", lambda *_: (1, 1))
        assert_tree_bits(control, window.materialize(torch.device("cuda")).cpu())
        assert source._cache._live_device_bytes == 0
        assert window.transport_stats["host_payload_bytes"] > 0


@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_prepared_full_shape_prefix_preserves_outputs_and_encoder_generation(tmp_path, device):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    from stockagent.models.factorized_input import forward_factorized_live_window
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path, host_cache_bytes=4096)
    panel.features.base.setflags(write=False)
    panel.features._live_prepared_packet_owner = True
    window = FactorizedLiveWindow(FactorizedDateView(panel.features, [1, 3, 4, 5, 6]), 1, 5)
    model = make_model().to(device)
    mask = torch.tensor([[True, False, True]], device=device)
    with torch.inference_mode(), torch.autocast(device, dtype=torch.bfloat16, enabled=device == "cuda"):
        control = call_model(model, window, mask, return_aux=True)
        first = forward_factorized_live_window(model, window, mask, return_aux=True, prepared_full_shape_prefix=True)
        assert_tree_bits(control, first)
        assert window.transport_stats["prepared_full_shape_prefix_hit"] is False
        changed = window.with_last_feature(1, [-.02, -0., .025])
        control = call_model(model, changed, mask, return_aux=True)
        candidate = forward_factorized_live_window(model, changed, mask, return_aux=True, prepared_full_shape_prefix=True)
        assert_tree_bits(control, candidate)
        assert changed.transport_stats["prepared_full_shape_prefix_hit"] is True
        # The ordinary full-shape encoder recomputes the actual final row;
        # no placeholder prefix ever reaches temporal/portfolio inference.
        model.candle_encoder.joint_projection.proj.weight.add_(.1)
        control = call_model(model, changed, mask, return_aux=True)
        candidate = forward_factorized_live_window(model, changed, mask, return_aux=True, prepared_full_shape_prefix=True)
        assert_tree_bits(control, candidate)
        assert changed.transport_stats["prepared_full_shape_prefix_hit"] is False
        panel.features.base.setflags(write=True)
        panel.features.base[4, 0, 0] += .3
        control = call_model(model, changed, mask, return_aux=True)
        candidate = forward_factorized_live_window(model, changed, mask, return_aux=True, prepared_full_shape_prefix=True)
        assert_tree_bits(control, candidate)
        assert changed.transport_stats["prepared_full_shape_prefix_hit"] is False
        assert "_live_full_shape_prefix" not in model.__dict__


def test_owned_candle_requires_explicit_inference_and_plain_rms():
    model = make_model()
    x = torch.ones(1, 4, 3, 10)
    with pytest.raises(RuntimeError, match="plain FP32 inference"):
        model.candle_encoder(x, owned_inference=True)
    with torch.inference_mode():
        control, _ = model.candle_encoder(x.clone())
        candidate, _ = model.candle_encoder(x.clone(), owned_inference=True)
        assert_tree_bits(control, candidate)


def test_prepared_full_shape_prefix_lifetime_is_the_model_lifetime(tmp_path):
    import gc
    import weakref
    from stockagent.models import factorized_input
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path, host_cache_bytes=4096)
    panel.features.base.setflags(write=False)
    panel.features._live_prepared_packet_owner = True
    window = FactorizedLiveWindow(panel.features, 0, 4)
    model = make_model()
    mask = torch.tensor([[True, False, True]])
    with torch.inference_mode():
        factorized_input.forward_factorized_live_window(model, window, mask, prepared_full_shape_prefix=True)
        cached = weakref.ref(model._live_full_shape_prefix[2])
        assert model._live_full_shape_prefix[2].shape[1] == model.lookback - 1
        del model
        gc.collect()
        assert cached() is None  # No global learned-output cache defeats idle eviction.


@pytest.mark.parametrize("chunk_stocks", [1, 2])
def test_live_direct_destination_preserves_raw_bits_without_chunk_copy(tmp_path, monkeypatch, chunk_stocks):
    if not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    panel, path, dense = example(tmp_path)
    panel.features[4, 0, 0] = dense[4, 0, 0] = -0.
    panel.features[5, 1, 1] = dense[5, 1, 1] = float("nan")
    panel.features[6, 2, 0] = dense[6, 2, 0] = float("inf")
    attach_factorized_features(panel, path)
    view = FactorizedDateView(panel.features, [1, 3, 4, 5, 6])
    window = FactorizedLiveWindow(view, 1, 5).with_last_feature(1, [-.02, -0., .025])
    original = view[1:5].copy()
    expected = np.nan_to_num(dense[[3, 4, 5, 6]], nan=0., posinf=0., neginf=0.)
    expected[-1, :, 1] = window.overlay[1]
    destinations = []
    stock_chunk = FactorizedPanelSlab.stock_chunk
    stock_chunks = FactorizedLiveWindow.stock_chunks

    def chunks(self, device, **kwargs):
        return stock_chunks(self, device, chunk_stocks=chunk_stocks, **kwargs)

    def chunk(self, begin, end, **kwargs):
        destination = kwargs.get("destination")
        assert destination is not None and not destination.requires_grad
        result = stock_chunk(self, begin, end, **kwargs)
        assert result.data_ptr() == destination.data_ptr()
        destinations.append((begin, end, result.stride()))
        return result

    monkeypatch.setattr(FactorizedLiveWindow, "stock_chunks", chunks)
    monkeypatch.setattr(FactorizedPanelSlab, "stock_chunk", chunk)
    with torch.inference_mode():
        actual = window.materialize(torch.device("cuda"), direct_destination=True)
        assert actual.is_contiguous() and actual.dtype == torch.float32
        assert actual.cpu().numpy()[0].tobytes() == expected.tobytes()
        # Reusing an input must never reuse or mutate another call's output.
        actual.fill_(9.)
        assert window.materialize(torch.device("cuda"), direct_destination=True).cpu().numpy()[0].tobytes() == expected.tobytes()
    assert len(destinations) == 2*((3+chunk_stocks-1)//chunk_stocks)
    assert any(stride[0] > (end-begin)*10 for begin, end, stride in destinations)
    assert window.transport_stats["avoided_dense_copy_bytes"] == expected.nbytes
    assert window.transport_stats["destination_policy"] == "owned_full_window_slices_v1"
    assert view[1:5].tobytes() == original.tobytes()


def test_direct_assembly_is_opt_in_and_requires_cuda_inference(tmp_path):
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path)
    window = FactorizedLiveWindow(panel.features, 1, 5)
    window.materialize(torch.device("cpu"))
    assert window.transport_stats["destination_policy"] == "private_chunk_then_copy_v1"
    assert window.transport_stats["avoided_dense_copy_bytes"] == 0
    assert window.transport_stats["prepared_host_packets_active"] is False
    with torch.inference_mode(), pytest.raises(RuntimeError, match="CUDA inference"):
        window.materialize(torch.device("cpu"), direct_destination=True)
    with pytest.raises(RuntimeError, match="CUDA inference"):
        window.materialize(torch.device("cuda"), direct_destination=True)
    if torch.cuda.is_available():
        with torch.inference_mode():
            window.materialize(torch.device("cuda"))
        assert window.transport_stats["destination_policy"] == "private_chunk_then_copy_v1"


def test_slab_destination_rejects_training_and_invalid_layout_without_mutation(tmp_path):
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path)
    slab = FactorizedPanelSlab(panel.features, [1, 3, 4, 5], device="cpu")
    destination = torch.full((4, 1, 10), 7.)
    with pytest.raises(RuntimeError, match="inference mode"):
        slab.stock_chunk(0, 1, destination=destination)
    with torch.inference_mode(), pytest.raises(ValueError, match="CUDA FP32"):
        slab.stock_chunk(0, 1, destination=destination)
    assert torch.all(destination == 7.)
    if torch.cuda.is_available():
        slab = FactorizedPanelSlab(panel.features, [1, 3, 4, 5], device="cuda")
        panel.features.transfer_mode = "compact_cuda_packed"
        with torch.inference_mode():
            invalid = [torch.full((4, 1, 10), 7., device="cuda", dtype=torch.float64),
                       torch.full((4, 1, 20), 7., device="cuda")[..., ::2],
                       torch.full((1, 1, 10), 7., device="cuda").expand(4, 1, 10)]
            for value in invalid:
                with pytest.raises(ValueError, match="nonoverlapping"):
                    slab.stock_chunk(0, 1, destination=value)
                assert torch.all(value == 7.)


def test_live_window_rejects_training_before_any_source_read(tmp_path):
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path)
    window = FactorizedLiveWindow(panel.features, 1, 5)
    with pytest.raises(RuntimeError, match="eval and inference"):
        call_model(make_model().train(), window, torch.ones(1, 3, dtype=torch.bool))
    with pytest.raises(ValueError, match="outside the source calendar"):
        FactorizedLiveWindow(panel.features, -1, 3)
    with pytest.raises(ValueError, match="symbol-aligned"):
        window.with_last_feature(0, [float("nan")]*3)


def test_only_matching_plain_factorized_model_admits_transport():
    from dataclasses import replace
    from stockagent.config import load_config
    from stockagent.live.signal_engine import _factorized_live_stream_admitted
    config = load_config("configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_live_v1.yaml")
    assert _factorized_live_stream_admitted(config)
    for delta in ({"causal_feature_rms_normalization": False},
                  {"causal_feature_window_rms_normalization": True},
                  {"temporal_basis_families": ["haar"]},
                  {"candle_dropout": .1}, {"feature_svd_components": 2}):
        other = replace(config, training=replace(config.training,
            financial_transformer=replace(config.training.financial_transformer, **delta)))
        assert not _factorized_live_stream_admitted(other)


def test_live_attachment_respects_existing_cache_budget(tmp_path):
    from stockagent.data.factorized_panel import attach_factorized_live_features
    panel, path, _ = example(tmp_path)
    import json
    manifest = json.loads(path.read_text())
    manifest["paper_inference"] = {"contract": "factorized_paper_projection_lag1_v1",
        "status": "verified", "raw_source_publication": False}
    path.write_text(json.dumps(manifest))
    attached = attach_factorized_live_features(panel, path, host_cache_bytes=4096)
    assert attached.features.source._cache.budget_bytes == 4096
    assert attached.features.source._live_prepared_packet_owner is True
    assert attached.features.source.base.flags.writeable is False
    assert attached.features.source.common.flags.writeable is False


@pytest.mark.parametrize("workers", [1, 2])
def test_member_validation_reports_real_progress_and_keeps_every_sha(tmp_path, workers):
    import json
    from stockagent.data.factorized_panel import factorized_member_proofs, verify_factorized_members
    _, path, _ = example(tmp_path)
    manifest = json.loads(path.read_bytes())
    events = []
    verify_factorized_members(path.parent, manifest, workers=workers,
                              progress_callback=lambda *event: events.append(event))
    count = len(factorized_member_proofs(manifest))
    assert events == [("factorized_member_verify", done, count) for done in range(count + 1)]
    (path.parent / manifest["common"]["path"]).write_bytes(b"corrupt")
    events.clear()
    with pytest.raises(ValueError, match="member identity mismatch"):
        verify_factorized_members(path.parent, manifest, workers=workers,
                                  progress_callback=lambda *event: events.append(event))
    assert events == [("factorized_member_verify", 0, count)]


def test_member_progress_observer_cannot_fail_valid_source(tmp_path):
    import json
    from stockagent.data.factorized_panel import verify_factorized_members
    _, path, _ = example(tmp_path)

    def broken_observer(*_):
        raise OSError("monitor unavailable")

    verify_factorized_members(path.parent, json.loads(path.read_bytes()),
                              progress_callback=broken_observer)


@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_opening_adapter_preserves_complete_window_and_missing_price_sentinel(tmp_path, device):
    from stockagent.live.quote_provider import PriceSnapshot
    from stockagent.live.signal_engine import (
        DAY_TRADE_OPEN_GAP_FEATURE, _day_trade_live_model_window,
    )
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    panel, path, dense = example(tmp_path)
    attach_factorized_features(panel, path)
    panel.feature_names[1] = DAY_TRADE_OPEN_GAP_FEATURE
    base = dense[-4:].copy()
    before = base.copy()
    lazy = FactorizedLiveWindow(panel.features, 5, 9)
    snapshot = PriceSnapshot(prices=np.array([1.05, np.nan, 1.04]),
        open_prices=np.array([1.05, np.nan, 1.04]), source="shioaji",
        timestamp="2025-01-10 09:00:00")
    kwargs = dict(panel_idx=8, lookback=4, price_snapshot=snapshot,
        resolved_asof="2025-01-10 09:00:00", source_timezone="Asia/Taipei")
    control = _day_trade_live_model_window(panel, base_model_window=base, **kwargs)
    candidate = _day_trade_live_model_window(panel, base_model_window=lazy, **kwargs)
    assert candidate[1:4] == control[1:4]
    assert candidate[4].tobytes() == control[4].tobytes()
    assert candidate[4].tolist() == [True, False, True]
    assert candidate[0].materialize(torch.device(device)).cpu().numpy()[0].tobytes() == control[0].tobytes()
    assert control[0][-1, 1, 1] == 0.  # Unknown price is not an invented open.
    assert base.tobytes() == before.tobytes()
    assert panel.features[-4:].tobytes() == before.tobytes()


@pytest.mark.parametrize("attribute,value", [
    ("causal_feature_rms_normalization", False),
    ("causal_feature_compression", "log1p"),
    ("causal_feature_window_rms_normalization", True),
    ("feature_svd_components", 2), ("feature_bottleneck_dim", 2),
])
def test_live_model_guard_rejects_other_feature_transforms_before_io(tmp_path, attribute, value):
    panel, path, _ = example(tmp_path)
    attach_factorized_features(panel, path)
    window = FactorizedLiveWindow(panel.features, 1, 5)
    model = make_model()
    setattr(model.candle_encoder, attribute, value)
    with torch.inference_mode(), pytest.raises(ValueError, match="admitted no-basis"):
        call_model(model, window, torch.ones(1, 3, dtype=torch.bool))
    assert panel.features._cache.misses == 0

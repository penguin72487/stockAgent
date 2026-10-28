"""Complete input reuse must retain canonical contracts and bounded ownership."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import date
import os
import threading
from types import SimpleNamespace

import numpy as np

import pytest

from stockagent.live import signal_engine as engine
from test_config_snapshot_cache import _inherited
from test_factorized_panel import example


def _rule_source(tmp_path):
    from test_day_trade_execution_reconciliation import _rules
    root = tmp_path / "stocks"
    root.mkdir()
    (root / "symbols.csv").write_text("code,market,security_type,name\n2330,twse,stock,A\n6733,tpex,stock,B\n")
    _rules(tmp_path, "twse", "2330")
    _rules(tmp_path, "tpex", "6733")
    return root


def test_six_checkpoint_axes_share_one_canonical_eligibility_read(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_simulation as execution
    root = _rule_source(tmp_path)
    canonical = execution.load_live_eligibility
    axes = [("2330",), ("6733",), ("2330", "6733"), ("6733", "2330"),
            ("unknown", "2330"), ("2330", "2330", "6733")]
    expected = [canonical(rule_data_dir=tmp_path, parquet_root=root, symbols=axis,
                          trading_date=date(2026, 8, 13), require_latest=False)[0] for axis in axes]
    started, release = threading.Event(), threading.Event()
    calls = []
    def paused(**kwargs):
        calls.append(1)
        started.set()
        assert release.wait(5)
        return canonical(**kwargs)
    monkeypatch.setattr(execution, "load_live_eligibility", paused)
    def run(axis):
        panel = SimpleNamespace(dates=np.array(["2026-08-12"], dtype="datetime64[D]"),
                                symbols=axis, day_trade_eligible_mask=None)
        return engine._day_trade_model_eligibility(panel, session_date="2026-08-13",
                    parquet_root=root, rule_data_dir=tmp_path)
    with ThreadPoolExecutor(max_workers=6) as pool:
        jobs = [pool.submit(run, axis) for axis in axes]
        assert started.wait(5)
        release.set()
        actual = [job.result(5) for job in jobs]
    assert calls == [1]
    for axis, rules, (eligible, short, proof) in zip(axes, expected, actual):
        assert eligible.tolist() == [rules[s].covered and rules[s].eligible for s in axis]
        assert short.tolist() == [rules[s].short_open for s in axis]
        assert proof["coverage"]["twse"]["covered"] is True
    actual[0][0][:] = False
    actual[0][2]["coverage"]["twse"]["covered"] = False
    assert run(axes[0])[0].tolist() == [True]
    assert run(axes[1])[2]["coverage"]["twse"]["covered"] is True


def test_axis_cache_hit_is_fenced_against_concurrent_source_replacement(tmp_path, monkeypatch):
    from test_day_trade_execution_reconciliation import _rules
    root = _rule_source(tmp_path)
    panel = SimpleNamespace(dates=np.array(["2026-08-12"], dtype="datetime64[D]"),
                            symbols=("2330", "6733"), day_trade_eligible_mask=None)
    kwargs = dict(session_date="2026-08-13", parquet_root=root, rule_data_dir=tmp_path)
    assert engine._day_trade_model_eligibility(panel, **kwargs)[0].tolist() == [True, True]
    cached = engine._cached_session_day_trade_rules
    def replace_after_hit(*args):
        result = cached(*args)
        _rules(tmp_path, "twse", "9999")
        return result
    monkeypatch.setattr(engine, "_cached_session_day_trade_rules", replace_after_hit)
    with pytest.raises(RuntimeError, match="changed during projection"):
        engine._day_trade_model_eligibility(panel, **kwargs)


def test_shared_rule_generation_detects_rewrite_and_retries_missing_source(tmp_path):
    from test_day_trade_execution_reconciliation import _rules
    root = _rule_source(tmp_path)
    paths = tmp_path / "twse_day_trade_eligibility.parquet"
    panel = SimpleNamespace(dates=np.array(["2026-08-12"], dtype="datetime64[D]"),
                            symbols=("2330", "6733"), day_trade_eligible_mask=None)
    def run(session="2026-08-13"):
        return engine._day_trade_model_eligibility(panel, session_date=session,
                    parquet_root=root, rule_data_dir=tmp_path)
    assert run()[0].tolist() == [True, True]
    before = paths.stat()
    _rules(tmp_path, "twse", "9999")
    os.utime(paths, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert run()[0].tolist() == [False, True]
    with pytest.raises(RuntimeError, match="eligibility unavailable"):
        run("2026-08-15")
    # A missing target session is not cached as an accepted empty universe.
    import polars as pl
    for venue, symbol in (("twse", "2330"), ("tpex", "6733")):
        pl.DataFrame({"date": ["2026-08-15"], "證券代號": [symbol],
            "暫停現股賣出後現款買進當沖註記": [""]}).write_parquet(tmp_path / f"{venue}_day_trade_eligibility.parquet")
    assert run("2026-08-15")[0].tolist() == [True, True]


def test_rule_source_change_during_load_is_not_cached_and_clear_fences_flight(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_simulation as execution
    from test_day_trade_execution_reconciliation import _rules
    root = _rule_source(tmp_path)
    canonical = execution.load_live_eligibility
    identity = engine._session_rule_source_identity(str(tmp_path), str(root))
    def rewrite(**kwargs):
        result = canonical(**kwargs)
        _rules(tmp_path, "twse", "9999")
        return result
    monkeypatch.setattr(execution, "load_live_eligibility", rewrite)
    with pytest.raises(RuntimeError, match="source changed"):
        engine._session_day_trade_rule_universe(str(tmp_path), str(root), "2026-08-13", identity)
    assert not engine._LIVE_SESSION_RULE_SOURCE_CACHE
    started, release = threading.Event(), threading.Event()
    def pause(**kwargs):
        started.set()
        assert release.wait(5)
        return canonical(**kwargs)
    monkeypatch.setattr(execution, "load_live_eligibility", pause)
    identity = engine._session_rule_source_identity(str(tmp_path), str(root))
    with ThreadPoolExecutor(max_workers=1) as pool:
        job = pool.submit(engine._session_day_trade_rule_universe, str(tmp_path), str(root), "2026-08-13", identity)
        assert started.wait(5)
        engine.clear_live_panel_memory_cache()
        release.set()
        assert job.result(5)[0]["6733"].eligible
    assert not engine._LIVE_SESSION_RULE_SOURCE_CACHE and not engine._LIVE_PANEL_BUILDS


def _normalization_panel():
    values = np.array([np.nan, -0.0, np.inf, -np.inf, 1.234567890123, 0.0] * 12, dtype=np.float64)
    return SimpleNamespace(features=values.reshape(3, 4, 6), num_symbols=4, feature_names=list("abcdef"))


def test_six_cold_windows_normalize_once_with_complete_bit_parity(monkeypatch):
    panel = _normalization_panel()
    source = panel.features.tobytes()
    expected = np.nan_to_num(panel.features, nan=0., posinf=0., neginf=0.).astype(np.float32)
    original = engine._normalize_model_window
    started, release = threading.Event(), threading.Event()
    calls = []

    def paused(*args, **kwargs):
        calls.append(1)
        started.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(engine, "_normalize_model_window", paused)
    with ThreadPoolExecutor(max_workers=6) as pool:
        jobs = [pool.submit(engine._cached_normalized_model_window, panel, panel_idx=2, lookback=3)
                for _ in range(6)]
        assert started.wait(5)
        release.set()
        results = [job.result(5) for job in jobs]
    assert calls == [1]
    assert all(result is results[0] for result in results)
    assert all(result.tobytes() == expected.tobytes() and not result.flags.writeable for result in results)
    assert panel.features.tobytes() == source


def test_normalization_clear_fences_flight_and_failure_is_retryable(monkeypatch):
    panel = _normalization_panel()
    original = engine._normalize_model_window
    started, release = threading.Event(), threading.Event()

    def paused(*args, **kwargs):
        started.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(engine, "_normalize_model_window", paused)
    with ThreadPoolExecutor(max_workers=1) as pool:
        job = pool.submit(engine._cached_normalized_model_window, panel, panel_idx=2, lookback=3)
        assert started.wait(5)
        engine.clear_live_panel_memory_cache()
        release.set()
        assert job.result(5).shape == (3, 4, 6)
    assert not engine._LIVE_MODEL_INPUT_CACHE and not engine._LIVE_PANEL_BUILDS
    def fail(*args, **kwargs):
        raise OSError("normalization failed")
    monkeypatch.setattr(engine, "_normalize_model_window", fail)
    with pytest.raises(OSError, match="normalization failed"):
        engine._cached_normalized_model_window(panel, panel_idx=2, lookback=3)
    assert not engine._LIVE_MODEL_INPUT_CACHE and not engine._LIVE_PANEL_BUILDS
    monkeypatch.setattr(engine, "_normalize_model_window", original)
    assert engine._cached_normalized_model_window(panel, panel_idx=2, lookback=3).shape == (3, 4, 6)


def test_window_budget_and_source_identity_do_not_share_incompatible_arrays(monkeypatch):
    first, second = _normalization_panel(), _normalization_panel()
    second.features[0, 0, 0] = 99.
    a = engine._cached_normalized_model_window(first, panel_idx=2, lookback=3)
    b = engine._cached_normalized_model_window(second, panel_idx=2, lookback=3)
    assert a is not b and a[0, 0, 0] == 0. and b[0, 0, 0] == 99.
    engine.clear_live_panel_memory_cache()
    monkeypatch.setattr(engine, "_LIVE_MODEL_INPUT_CACHE_MAX_BYTES", 1)
    c = engine._cached_normalized_model_window(first, panel_idx=2, lookback=3)
    assert c.tobytes() == a.tobytes() and not engine._LIVE_MODEL_INPUT_CACHE


def test_last_trade_preparation_shares_one_flight_and_clear_cannot_resurrect_it(monkeypatch):
    panel = SimpleNamespace(num_symbols=2, close_prices=np.array([[10., 20.], [11., np.nan]]),
        daily_volumes=np.array([[100., 200.], [100., 0.]]), alive_mask=np.ones((2, 2), dtype=bool),
        dates=np.array(["2026-10-05", "2026-10-06"], dtype="datetime64[D]"))
    original = engine._compute_last_completed_trade_reference
    expected = original(panel, 1)
    started, release = threading.Event(), threading.Event()
    calls = []
    def paused(*args, **kwargs):
        calls.append(1)
        started.set()
        assert release.wait(5)
        return original(*args, **kwargs)
    monkeypatch.setattr(engine, "_compute_last_completed_trade_reference", paused)
    with ThreadPoolExecutor(max_workers=6) as pool:
        jobs = [pool.submit(engine._last_completed_trade_reference, panel, 1) for _ in range(6)]
        assert started.wait(5)
        release.set()
        results = [job.result(5) for job in jobs]
    assert calls == [1] and all(result is results[0] for result in results)
    assert all(a.tobytes() == b.tobytes() for a, b in zip(results[0], expected))
    engine.clear_live_panel_memory_cache()
    started.clear()
    release.clear()
    with ThreadPoolExecutor(max_workers=1) as pool:
        job = pool.submit(engine._last_completed_trade_reference, panel, 1)
        assert started.wait(5)
        engine.clear_live_panel_memory_cache()
        release.set()
        assert all(a.tobytes() == b.tobytes() for a, b in zip(job.result(5), expected))
    assert not engine._LIVE_LAST_TRADE_REFERENCE_CACHE and not engine._LIVE_PANEL_BUILDS


@pytest.fixture(autouse=True)
def isolated():
    engine.clear_live_inference_memory_cache()
    yield
    engine.clear_live_inference_memory_cache()


def inputs(tmp_path):
    _, _, path = _inherited(tmp_path)
    config = engine.load_config(path)
    config.trading.execution_mode = "tw_day_trade"
    config.training.model_name = "financial_transformer"
    panel, _, _ = example(tmp_path)
    return config, panel, {"experiment_manifest": {"schema_version": 4}}


def test_live_loader_uses_canonical_transitive_snapshot_and_idle_releases_it(tmp_path, monkeypatch):
    from stockagent import config as owner
    _, _, path = _inherited(tmp_path)
    calls, canonical = [], owner.load_config

    def counted(*args, **kwargs):
        calls.append(1)
        return canonical(*args, **kwargs)

    monkeypatch.setattr(owner, "load_config", counted)
    first = engine.load_config(path)
    expected = asdict(first)
    first.environment.device = "cpu"
    assert asdict(engine.load_config(path)) == expected
    assert len(calls) == 1
    engine.clear_live_inference_memory_cache()
    assert not owner._CONFIG_SNAPSHOT_CACHE
    assert asdict(engine.load_config(path)) == expected
    assert len(calls) == 2


def test_preopen_loader_keeps_transitive_source_signature_contract(tmp_path):
    _, _, path = _inherited(tmp_path)
    sources = {}
    engine.load_config(path, source_signatures=sources)
    assert {entry.name for entry in sources} == {"base.yaml", "middle.yaml", "child.yaml"}
    from stockagent import config as owner
    assert not owner._CONFIG_SNAPSHOT_CACHE


def test_complete_expected_contract_equal_and_result_not_shared(tmp_path, monkeypatch):
    config, panel, checkpoint = inputs(tmp_path)
    build = engine.build_inference_checkpoint_manifest
    expected = build(panel, config, checkpoint=checkpoint)
    calls = []

    def counted(*args, **kwargs):
        calls.append(1)
        return build(*args, **kwargs)

    monkeypatch.setattr(engine, "build_inference_checkpoint_manifest", counted)
    first, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert first == expected and not hit
    first["contracts"]["model"]["symbols"][0] = "MUTATED"
    second, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert second == expected and hit
    assert calls == [1]


@pytest.mark.parametrize("change", ["config", "symbols", "features", "environment"])
def test_every_consumed_axis_invalidates_expected_semantics(tmp_path, monkeypatch, change):
    config, panel, checkpoint = inputs(tmp_path)
    engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    if change == "config":
        config.training.lookback += 1
    elif change == "symbols":
        panel.symbols[0] = "NEW"
    elif change == "features":
        panel.feature_names[0] = "NEW"
    else:
        monkeypatch.setenv("STOCKAGENT_PREPARED_INPUT_CONTRACT_TEST", "changed")
    expected = engine.build_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    actual, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert actual == expected and not hit


def test_fresh_checkpoint_is_validated_again_even_when_expected_contract_reused(tmp_path):
    config, panel, checkpoint = inputs(tmp_path)
    expected, _ = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    checkpoint["experiment_manifest"] = expected
    again, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert hit
    engine.validate_checkpoint_manifest(checkpoint, again, checkpoint_path=tmp_path / "checkpoint.pt", scope="model")
    checkpoint["experiment_manifest"]["fingerprints"]["model"] = "0" * 64
    again, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert hit
    with pytest.raises((ValueError, RuntimeError), match="(?i)model|fingerprint"):
        engine.validate_checkpoint_manifest(checkpoint, again, checkpoint_path=tmp_path / "checkpoint.pt", scope="model")


def test_changing_quote_receipt_root_does_not_invalidate_non_quote_model_semantics(tmp_path, monkeypatch):
    config, panel, checkpoint = inputs(tmp_path)
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "first"))
    expected, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert not hit
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "second"))
    actual, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert hit and actual == expected
    assert actual == engine.build_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)


@pytest.mark.parametrize("schema", [None, 1, 2, 3, True, "4"])
def test_legacy_or_unknown_schema_keeps_full_builder(tmp_path, monkeypatch, schema):
    config, panel, checkpoint = inputs(tmp_path)
    checkpoint["experiment_manifest"]["schema_version"] = schema
    calls = []
    monkeypatch.setattr(engine, "build_inference_checkpoint_manifest", lambda *a, **kw: calls.append(1) or {"full": True})
    for _ in range(2):
        assert engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint) == ({"full": True}, False)
    assert calls == [1, 1] and not engine._LIVE_CHECKPOINT_MANIFEST_CACHE


def test_cache_holds_only_bounded_bytes_and_oversize_is_never_admitted(tmp_path, monkeypatch):
    config, panel, checkpoint = inputs(tmp_path)
    monkeypatch.setattr(engine, "_LIVE_CHECKPOINT_MANIFEST_CACHE_MAX_BYTES", 32)
    monkeypatch.setattr(engine, "build_inference_checkpoint_manifest", lambda *a, **kw: {"complete": "x" * 64})
    for _ in range(2):
        value, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
        assert value == {"complete": "x" * 64} and not hit
        assert not engine._LIVE_CHECKPOINT_MANIFEST_CACHE


def test_offline_control_and_oracle_builders_are_not_hidden_by_candidate_cache(tmp_path, monkeypatch):
    config, panel, checkpoint = inputs(tmp_path)
    first, _ = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    calls = []
    monkeypatch.setattr(engine, "build_inference_checkpoint_manifest", lambda *a, **kw: calls.append(1) or {"complete_control": first})
    control, hit = engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert control == {"complete_control": first} and not hit and calls == [1]


def test_concurrent_immutable_build_is_shared_and_clear_fences_old_flight(tmp_path, monkeypatch):
    config, panel, checkpoint = inputs(tmp_path)
    started, release = threading.Event(), threading.Event()
    calls = []

    def build(*a, **kw):
        calls.append(1)
        started.set()
        assert release.wait(3)
        return {"complete": True}

    monkeypatch.setattr(engine, "build_inference_checkpoint_manifest", build)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(engine._cached_inference_checkpoint_manifest, panel, config, checkpoint=checkpoint)
        assert started.wait(3)
        engine.clear_live_panel_memory_cache()
        release.set()
        assert first.result(3) == ({"complete": True}, False)
    assert not engine._LIVE_CHECKPOINT_MANIFEST_CACHE
    assert engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint) == ({"complete": True}, False)
    assert calls == [1, 1]
    assert all(isinstance(value, bytes) for value in engine._LIVE_CHECKPOINT_MANIFEST_CACHE.values())


def test_failed_build_is_not_cached_or_replaced_by_old_success(tmp_path, monkeypatch):
    config, panel, checkpoint = inputs(tmp_path)
    monkeypatch.setattr(engine, "build_inference_checkpoint_manifest", lambda *a, **kw: (_ for _ in ()).throw(ValueError("invalid inputs")))
    with pytest.raises(ValueError, match="invalid inputs"):
        engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint)
    assert not engine._LIVE_CHECKPOINT_MANIFEST_CACHE and not engine._LIVE_PANEL_BUILDS
    monkeypatch.setattr(engine, "build_inference_checkpoint_manifest", lambda *a, **kw: {"complete": True})
    assert engine._cached_inference_checkpoint_manifest(panel, config, checkpoint=checkpoint) == ({"complete": True}, False)


def test_idle_evicts_actual_factorized_attachment_pair_and_retires_packets(tmp_path, monkeypatch):
    from test_live_panel_singleflight import factorized_example
    panel, _path, _dense, config = factorized_example(tmp_path)
    attached = engine._cached_factorized_live_panel(panel, config)
    calls = []
    from stockagent.data.factorized_panel import FactorizedDateView
    features = attached.features.source if isinstance(attached.features, FactorizedDateView) else attached.features
    monkeypatch.setattr(features._cache, "retire_live_device_packets", lambda: calls.append(1) or True)
    monkeypatch.setattr(engine, "_LIVE_CUDA_CACHE_RELEASE_PENDING", False)
    assert next(iter(engine._LIVE_FACTORIZED_PANEL_CACHE.values())) == (panel, attached)
    engine.clear_live_panel_memory_cache()
    assert calls == [1]
    assert not engine._LIVE_FACTORIZED_PANEL_CACHE
    assert engine._LIVE_CUDA_CACHE_RELEASE_PENDING is True

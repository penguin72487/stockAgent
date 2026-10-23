import pytest
import torch
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace

from scripts.benchmark_opening_signal_pipeline import (
    _candle_aux_forward, _candle_aux_oracle, _hold_source_read_leases, _measurement_summary,
    _model_contract_oracle,
    _publication_offset_ms,
    _benchmark_runtime_profile, _require_selected_sparse_runtime,
    _preopen_kwargs, _input_reference_evidence, _receipt_universe,
)


def test_preopen_preparation_never_consumes_opening_quotes_or_writes():
    original = {"price_source": "tw", "write": True, "asof_date": "2026-10-22 09:00:32",
                "panel_date": "2026-10-21", "checkpoint": "same"}
    kwargs = _preopen_kwargs(original, "2026-10-22")
    assert kwargs["price_source"] == "panel" and kwargs["write"] is False
    assert kwargs["asof_date"] == "2026-10-22 08:50:00"
    assert kwargs["checkpoint"] == "same" and kwargs["panel_date"] == "2026-10-21"
    assert original["write"] is True and original["price_source"] == "tw"


def test_historical_receipt_registry_reuses_full_validation_at_original_clock(monkeypatch):
    from stockagent.data import tw_current_trading_universe as registry
    original_clock, observed = registry.datetime, []
    def validate(day):
        observed.append((day, registry.datetime.now(registry.TAIPEI).isoformat()))
        return {"canonical_validation_not_bypassed": True}
    monkeypatch.setattr(registry, "load_current_universe", validate)
    result = _receipt_universe({"session_date": "2026-10-22", "captured_at_taipei": "2026-10-22T09:00:59+08:00"})
    assert result == {"canonical_validation_not_bypassed": True}
    assert observed == [("2026-10-22", "2026-10-22T09:00:59+08:00")]
    assert registry.datetime is original_clock


def test_reference_readiness_counts_carried_last_trade_without_claiming_a_fill():
    result = SimpleNamespace(summary={"opening_price_active_symbol_count": 3,
        "opening_price_active_count": 1, "opening_no_print_active_count": 1,
        "opening_no_print_reference_unavailable_count": 1, "opening_input_resolved_count": 2,
        "opening_input_required_count": 2}, weights_rows=[{"symbol": "ZERO",
            "model_input_price": 90., "model_input_price_is_carry": True, "open_price": None,
            "model_input_price_source_date": "2026-10-21", "secret": "not-copied"}])
    proof = _input_reference_evidence(result)
    assert proof["resolved_model_input_count"] == 2 and proof["carried_reference_active_count"] == 1
    assert proof["no_first_reference_unavailable_count"] == 1
    assert proof["rows"][0]["open_price"] is None and "secret" not in proof["rows"][0]


def test_benchmark_runtime_profile_is_explicit_private_key_free_and_never_overrides_conflicts(tmp_path, monkeypatch):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    keys = benchmark._RUNTIME_PROFILE_KEYS
    for key in keys:
        monkeypatch.setenv(key, "")
        monkeypatch.delenv(key, raising=False)
    path = tmp_path / "exact-runtime.env"
    path.write_text("STOCKAGENT_SPARSE_ADJOINT_BACKEND=cusparse_csr_transpose\nSTOCKAGENT_SPARSE_ADJOINT_ALGORITHM=6\n")
    proof = _benchmark_runtime_profile(path)
    assert proof["values"]["STOCKAGENT_SPARSE_ADJOINT_ALGORITHM"] == "6"
    assert len(proof["sha256"]) == 64
    monkeypatch.setenv("STOCKAGENT_SPARSE_ADJOINT_ALGORITHM", "different")
    with pytest.raises(ValueError, match="conflicts"):
        _benchmark_runtime_profile(path)
    path.write_text("DISCORD_BOT_TOKEN=do-not-load\n")
    with pytest.raises(ValueError, match="non-secret"):
        _benchmark_runtime_profile(path)


def test_missing_sparse_runtime_fails_before_panels_or_model_inference(monkeypatch):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    from stockagent.training import checkpoint_contract
    monkeypatch.setattr(benchmark.config_module, "load_config", lambda _: None)
    monkeypatch.setattr(checkpoint_contract, "_active_model_config", lambda _: {"values": {"factorized_sparse_projection": True}})
    for key in benchmark._RUNTIME_PROFILE_KEYS:
        monkeypatch.delenv(key, raising=False)
    cfgs = [SimpleNamespace(market="fixture", config_path="unused")]
    with pytest.raises(ValueError, match="--runtime-profile"):
        _require_selected_sparse_runtime(cfgs)
    for key in benchmark._RUNTIME_PROFILE_KEYS:
        monkeypatch.setenv(key, "explicit")
    _require_selected_sparse_runtime(cfgs)
    monkeypatch.setattr(checkpoint_contract, "_active_model_config", lambda _: {"values": {}})
    for key in benchmark._RUNTIME_PROFILE_KEYS:
        monkeypatch.delenv(key, raising=False)
    _require_selected_sparse_runtime(cfgs)


def test_full_benchmark_leases_each_canonical_source_once_until_samples_finish(tmp_path, monkeypatch):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    calls = []

    @contextmanager
    def guard(root):
        calls.append(("acquire", root))
        try:
            yield
        finally:
            calls.append(("release", root))

    roots = [str(tmp_path / "second"), str(tmp_path / "first"), str(tmp_path / "first/../second")]
    monkeypatch.setattr(benchmark.config_module, "load_config", lambda path:
                        SimpleNamespace(data=SimpleNamespace(parquet_root=roots[path])))
    monkeypatch.setattr(benchmark.signal_engine, "live_tw_public_source_read_guard", guard)
    with ExitStack() as stack:
        result = _hold_source_read_leases(stack, [SimpleNamespace(config_path=i) for i in range(3)])
        assert result == sorted(set(str(tmp_path / name) for name in ("first", "second")))
        assert calls == [("acquire", root) for root in result]
    assert calls == [("acquire", root) for root in result] + [("release", root) for root in result[::-1]]


def test_existing_writer_rejects_comparison_and_releases_already_held_sources(tmp_path, monkeypatch):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
    calls = []

    @contextmanager
    def guard(root):
        if root.endswith("second"):
            raise TWPublicSourceUpdatePending(tmp_path, tmp_path / "writer.lock")
        calls.append("acquired")
        try:
            yield
        finally:
            calls.append("released")

    monkeypatch.setattr(benchmark.config_module, "load_config", lambda name:
                        SimpleNamespace(data=SimpleNamespace(parquet_root=str(tmp_path / name))))
    monkeypatch.setattr(benchmark.signal_engine, "live_tw_public_source_read_guard", guard)
    with pytest.raises(TWPublicSourceUpdatePending), ExitStack() as stack:
        _hold_source_read_leases(stack, [SimpleNamespace(config_path=name) for name in ("first", "second")])
    assert calls == ["acquired", "released"]


def test_diagnostic_variant_changes_only_per_call_materialization():
    choices = []

    def forward(model, x, mask, **options):
        choices.append((model, x, mask, options))
        return "result"

    call = _candle_aux_forward(forward, materialize=False)
    assert call("model", "input", "mask", return_aux=True,
                materialize_candle_aux=True, symbol_indices="ids") == "result"
    assert choices == [("model", "input", "mask", {
        "return_aux": True, "materialize_candle_aux": False, "symbol_indices": "ids"})]


def test_source_wait_releases_partial_leases_before_retry(tmp_path, monkeypatch):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
    calls, second_attempts = [], []

    @contextmanager
    def guard(root):
        if root.endswith("second"):
            second_attempts.append(True)
            if len(second_attempts) == 1:
                raise TWPublicSourceUpdatePending(tmp_path, tmp_path / "writer.lock")
        calls.append(("acquire", root))
        try:
            yield
        finally:
            calls.append(("release", root))

    monkeypatch.setattr(benchmark.config_module, "load_config", lambda name:
                        SimpleNamespace(data=SimpleNamespace(parquet_root=str(tmp_path / name))))
    monkeypatch.setattr(benchmark.signal_engine, "live_tw_public_source_read_guard", guard)
    monkeypatch.setattr(benchmark.time, "sleep", lambda _: calls.append(("wait", None)))
    with ExitStack() as stack:
        _hold_source_read_leases(stack, [SimpleNamespace(config_path=name) for name in ("first", "second")], wait_seconds=1)
    assert calls[:3] == [("acquire", str(tmp_path / "first")),
                        ("release", str(tmp_path / "first")), ("wait", None)]
    assert len(second_attempts) == 2


@pytest.mark.parametrize("damage", [None, "weight", "parent_aux", "missing_field", "diagnostic"])
def test_candle_oracle_allows_only_declared_unmaterialized_fields(damage):
    class Model(torch.nn.Module):
        def candle_diagnostic_aux_fields(self):
            return ("candle_embedding",)

    choices, evidence = [], []

    def forward(model, x, mask, *, materialize_candle_aux, return_aux):
        assert return_aux is True
        choices.append(materialize_candle_aux)
        weights = torch.tensor([[.2, -.1]])
        aux = {"score_logits": weights.clone(), "stock_embedding": weights.clone(),
               "candle_embedding": weights.clone() if materialize_candle_aux else None}
        if not materialize_candle_aux:
            if damage == "weight":
                weights[0, 0] = .3
            elif damage == "parent_aux":
                aux["stock_embedding"][0, 0] = .3
            elif damage == "missing_field":
                aux.pop("candle_embedding")
            elif damage == "diagnostic":
                aux["candle_embedding"] = weights.clone()
        return weights, weights.clone(), aux

    oracle = _candle_aux_oracle(forward, evidence)
    if damage:
        with pytest.raises(RuntimeError):
            oracle(Model(), None, None, return_aux=True)
        assert evidence == []
    else:
        output = oracle(Model(), None, None, return_aux=True)
        assert output[2]["candle_embedding"] is None
        assert evidence[0]["all_other_tensor_bytes_equal"]
        assert len(evidence[0]["control_tensor_inventory"]) == 5
        assert len(evidence[0]["candidate_tensor_inventory"]) == 4
    assert choices == [True, False]


@pytest.mark.parametrize("damage", [None, "model", "data_schema", "compatibility"])
def test_manifest_oracle_preserves_current_and_compatible_fingerprints(monkeypatch, damage):
    import copy
    from scripts import benchmark_opening_signal_pipeline as benchmark
    from stockagent.training import checkpoint_contract
    compatible_keys = ("schema_4_pre_external_feature_source",
                       "schema_4_pre_projection_l1_active_count_scale",
                       "schema_4_with_inactive_futures_fields",
                       "schema_4_with_inactive_causal_feature_fields")
    full = {"fingerprints": {"model": "model", "data_schema": "schema", "data": "content"},
            "compatibility_fingerprints": {key: {"model": "compatible_model"} for key in compatible_keys},
            "contracts": {"model": {"model_name": "test"}}}
    full["compatibility_fingerprints"][compatible_keys[0]] = {"data_schema": "compatible_schema", "data": "old_content"}
    candidate = copy.deepcopy(full)
    candidate["validation_scope"] = "model"
    candidate["fingerprints"].pop("data")
    candidate["compatibility_fingerprints"][compatible_keys[0]].pop("data")
    if damage == "compatibility":
        candidate["compatibility_fingerprints"][compatible_keys[-1]]["model"] = "changed"
    elif damage:
        candidate["fingerprints"][damage] = "changed"
    calls, evidence = [], []
    monkeypatch.setattr(benchmark, "_full_inference_manifest", lambda *a, **kw: full)
    monkeypatch.setattr(checkpoint_contract, "validate_checkpoint_manifest",
                        lambda checkpoint, expected, **kw: calls.append((checkpoint, expected, kw["scope"])))
    oracle = _model_contract_oracle(lambda *a, **kw: candidate, evidence)
    if damage:
        with pytest.raises(RuntimeError):
            oracle(None, None, checkpoint="checkpoint")
        assert evidence == []
        assert calls == []
    else:
        assert oracle(None, None, checkpoint="checkpoint") is candidate
        assert calls == [("checkpoint", full, "model"), ("checkpoint", candidate, "model")]
        assert evidence[0]["current_and_compatible_fingerprints_equal"] is True


@pytest.mark.parametrize("accepted", [True, False, None])
def test_publication_clock_includes_queue_when_old_prefetch_was_not_consumed(accepted):
    result = SimpleNamespace(summary={"live_latency": {
        "quote_prefetched_outside_model_lock": accepted,
        "input_to_execution_pointer_ms": 200., "input_to_publish_ms": 250.,
    }})
    prefetched = SimpleNamespace(workflow_started=10.1)
    elapsed = _publication_offset_ms(result, batch_started=10., call_started=12., prefetched=prefetched)
    assert elapsed == pytest.approx(300. if accepted is True else 2200.)


def test_publication_clock_does_not_invent_a_missing_prefetch_origin():
    result = SimpleNamespace(summary={"live_latency": {
        "quote_prefetched_outside_model_lock": True, "input_to_publish_ms": 100.}})
    with pytest.raises(RuntimeError, match="no measured origin"):
        _publication_offset_ms(result, batch_started=1., call_started=2., prefetched=None)


def _sample(variant, published, complete, **cache_flags):
    stages = dict.fromkeys(("panel_cache_hit", "checkpoint_cache_hit",
                           "model_cache_hit", "alignment_cache_hit"), True)
    stages.update(cache_flags)
    return {"variant": variant, "all_published_ms": published,
            "all_artifacts_ms": complete, "markets": [{"stages": stages}]}


def test_summary_preserves_cold_sample_and_separates_complete_artifacts():
    records = [_sample("control", 100, 200),
               _sample("control", 10000, 12000, panel_cache_hit=False),
               _sample("candidate", 90, 180)]
    summary = _measurement_summary(records)
    assert summary["all_samples_retained"] is True
    control = summary["variants"]["control"]
    assert control["all_samples"] == {"sample_count": 2,
        "median_all_published_ms": 5050, "median_all_artifacts_ms": 6100,
        "min_all_published_ms": 100, "max_all_published_ms": 10000,
        "min_all_artifacts_ms": 200, "max_all_artifacts_ms": 12000}
    assert control["fully_warm_samples"] == {"sample_count": 1,
        "median_all_published_ms": 100, "median_all_artifacts_ms": 200,
        "min_all_published_ms": 100, "max_all_published_ms": 100,
        "min_all_artifacts_ms": 200, "max_all_artifacts_ms": 200}
    assert control["not_fully_warm_sample_count"] == 1
    assert len(records) == 3


def test_summary_does_not_invent_warm_latency_for_unknown_cache_state():
    sample = _sample("control", 10, 20, alignment_cache_hit=None)
    control = _measurement_summary([sample])["variants"]["control"]
    assert control["fully_warm_samples"] == {"sample_count": 0,
        "median_all_published_ms": None, "median_all_artifacts_ms": None,
        "min_all_published_ms": None, "max_all_published_ms": None,
        "min_all_artifacts_ms": None, "max_all_artifacts_ms": None}
    assert control["all_samples"]["sample_count"] == 1


def test_summary_empty_samples_remain_unknown():
    assert _measurement_summary([]) == {"all_samples_retained": True, "variants": {}}


def test_summary_never_merges_first_prepared_opening_with_steady_repeats():
    first = dict(_sample("candidate", 150, 200), sample_phase="first_opening_after_panel_only_preparation")
    steady = dict(_sample("candidate", 50, 100), sample_phase="steady_opening_repeat")
    result = _measurement_summary([first, steady])["variants"]["candidate"]
    assert result["all_samples"]["sample_count"] == 2
    assert result["first_opening_after_preparation"]["median_all_published_ms"] == 150
    assert result["steady_opening_repeats"]["median_all_published_ms"] == 50


def test_summary_separates_warm_caches_from_prefetch_admission():
    records = [_sample("control", 10, 20, quote_prefetched_outside_model_lock=flag)
               for flag in (True, False, None)]
    result = _measurement_summary(records)["variants"]["control"]
    assert result["fully_warm_samples"]["sample_count"] == 3
    assert result["prefetch_admission"] == {"adopted_model_count": 1,
        "fallback_model_count": 1, "unknown_model_count": 1}


def test_summary_legacy_prefetch_status_remains_unknown_not_success():
    result = _measurement_summary([_sample("legacy", 10, 20)])["variants"]["legacy"]
    assert result["prefetch_admission"] == {"adopted_model_count": 0,
        "fallback_model_count": 0, "unknown_model_count": 1}


def test_benchmark_manifest_binds_full_evidence_without_promoting_entry_rejection(tmp_path):
    import hashlib
    import json
    from scripts.benchmark_opening_signal_pipeline import _write_benchmark_manifest
    evidence = {"session": "2026-10-22", "input_observation_clock": "2026-10-22T13:29:59+08:00",
        "native_opening_to_completion_claim": False, "broker_fill_claim": False,
        "production_writes": False, "contracts": {"mode": "full-financial-output-digest"},
        "preopen_preparation": {"panel_only": True, "opening_quotes_used_for_preparation": False,
            "reference_calls": [{"phase": "opening", "elapsed_ms": 2., "same_reference_object_reused": True}],
            "reference_inventories": [{"prepared_during": "preopen", "source_cutoff": "2026-10-21",
                "symbols": ["A", "B", "C"], "prices": [100., None, 0.],
                "source_dates": ["2026-10-20", None, None]}]},
        "samples": [{"variant": "candidate", "sample_phase": "first_opening_after_panel_only_preparation",
            "all_published_ms": 100., "all_artifacts_ms": 120., "markets": [{"market": "mode",
                "stages": {"model_inference_ms": 2., "source_scope": ["keep-in-full-file"]},
                "input_reference_evidence": {"carried_reference_active_count": 1, "rows": [{"symbol": "A"}]},
                "execution_probe": {"status": "blocked", "position_count": 0, "filled_shares": None,
                                    "scope": "isolated_engineering_synthetic_one_lot_books_not_live_fill"}}]}]}
    path = tmp_path / "full.json"
    raw = json.dumps(evidence).encode()
    path.write_bytes(raw)
    manifest = _write_benchmark_manifest(path)
    result = json.loads(manifest.read_bytes())
    assert path.read_bytes() == raw
    assert result["source_artifact"] == {"path": str(path.resolve()), "bytes": len(raw),
                                         "sha256": hashlib.sha256(raw).hexdigest()}
    assert result["contracts"] == evidence["contracts"]
    assert result["native_opening_to_completion_claim"] is False
    assert result["preopen_preparation"]["reference_inventories"][0]["valid_reference_count"] == 1
    assert result["preopen_preparation"]["reference_calls"]["opening"]["same_reference_object_reused_count"] == 1
    sample = result["samples"][0]
    assert sample["sample_phase"] == "first_opening_after_panel_only_preparation"
    market = sample["markets"][0]
    assert market["stages"] == {"model_inference_ms": 2.}
    assert market["input_reference_evidence"] == {"carried_reference_active_count": 1}
    assert market["execution_probe"]["status"] == "blocked"
    assert market["execution_probe"]["filled_shares"] is None


def test_benchmark_manifest_preserves_failed_evidence_without_latency_success(tmp_path):
    import json
    from scripts.benchmark_opening_signal_pipeline import _write_benchmark_manifest
    path = tmp_path / "failure.json"
    path.write_text(json.dumps({"status": "failed", "stage": "opening_inference", "error_type": "CoveragePending"}))
    result = json.loads(_write_benchmark_manifest(path).read_bytes())
    assert result["status"] == "failed" and result["error_type"] == "CoveragePending"
    assert result["samples"] == [] and result["native_opening_to_completion_claim"] is None


def test_benchmark_manifest_rejects_replaced_source_after_readback(tmp_path, monkeypatch):
    from pathlib import Path
    from scripts.benchmark_opening_signal_pipeline import _write_benchmark_manifest
    source = tmp_path / "full.json"
    source.write_text("{}")
    different = tmp_path / "other.json"
    different.write_text("{}")
    original = Path.stat
    monkeypatch.setattr(Path, "stat", lambda path, **kw: original(different if path == source else path, **kw))
    with pytest.raises(RuntimeError, match="changed during manifest readback"):
        _write_benchmark_manifest(source)
    assert not source.with_name("full.manifest.json").exists()


@pytest.mark.parametrize("failed", [False, True])
def test_benchmark_input_trace_retains_real_phase_spans_even_on_failure(monkeypatch, failed):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    times = iter([1., 2., 2.5, 3.])
    monkeypatch.setattr(benchmark.time, "perf_counter", lambda: next(times))
    def prefetch(**kwargs):
        assert kwargs["config_path"] == "same-config" and kwargs["price_source"] == "tw"
        kwargs["progress_callback"]({"message": "panel_fingerprint"})
        kwargs["progress_callback"]({"message": "quote_transport"})
        kwargs["progress_callback"]({"message": "quote_transport"})
        if failed:
            raise ValueError("original source failure")
        return "unchanged-prefetch"
    monkeypatch.setattr(benchmark.signal_engine, "prefetch_live_signal_prices", prefetch)
    original = {"config_path": "same-config", "price_source": "tw"}
    stages = {}
    if failed:
        with pytest.raises(ValueError, match="original source failure"):
            benchmark._prefetch_with_stage_timing(original, stages)
    else:
        assert benchmark._prefetch_with_stage_timing(original, stages) == "unchanged-prefetch"
    assert stages == {"panel_fingerprint": 500., "quote_transport": 500.}
    assert original == {"config_path": "same-config", "price_source": "tw"}

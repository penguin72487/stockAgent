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


def test_isolated_signal_identity_is_an_explicit_clocked_input():
    from datetime import datetime, timedelta, timezone
    from scripts.benchmark_opening_signal_pipeline import _isolated_signal_id
    at = datetime(2026, 10, 22, 9, tzinfo=timezone(timedelta(hours=8)))
    first = _isolated_signal_id(market="mode", observed=at, scenario="0")
    assert first == _isolated_signal_id(market="mode", observed=at.astimezone(timezone.utc), scenario="0")
    assert first != _isolated_signal_id(market="other", observed=at, scenario="0")
    assert first != _isolated_signal_id(market="mode", observed=at, scenario="1")
    assert first != _isolated_signal_id(market="mode", observed=at + timedelta(microseconds=1), scenario="0")
    assert len({_isolated_signal_id(market=f"mode-{mode}", observed=at, scenario=str(revision))
                for mode in range(6) for revision in range(7)}) == 42
    assert first.startswith("engineering-opening-") and "/" not in first


@pytest.mark.parametrize("market,scenario,aware", [("", "0", True), ("mode", "", True), ("mode", "0", False)])
def test_isolated_signal_identity_rejects_missing_fixture_inputs(market, scenario, aware):
    from datetime import datetime, timezone
    from scripts.benchmark_opening_signal_pipeline import _isolated_signal_id
    with pytest.raises(ValueError, match="identity requires"):
        _isolated_signal_id(market=market, scenario=scenario,
            observed=datetime(2026, 10, 22, tzinfo=timezone.utc if aware else None))


def test_row_reference_executes_only_frozen_definitions_with_shared_canonical_globals(tmp_path, monkeypatch):
    import hashlib
    from scripts import benchmark_opening_signal_pipeline as benchmark
    path = tmp_path / "trusted-release.py"
    raw = ("raise RuntimeError('module startup must not execute')\n"
           "def generate_live_signal():\n    return _build_decision_rows()\n"
           "def _build_decision_rows():\n    return 'frozen-row-path'\n")
    path.write_text(raw)
    functions, proof = benchmark._reference_row_functions(path)
    assert functions["generate_live_signal"].__globals__ is vars(benchmark.signal_engine)
    monkeypatch.setattr(benchmark.signal_engine, "_build_decision_rows", functions["_build_decision_rows"])
    assert functions["generate_live_signal"]() == "frozen-row-path"
    monkeypatch.setattr(benchmark.signal_engine, "_build_decision_rows", lambda: "shared-clock-and-guards")
    assert functions["generate_live_signal"]() == "shared-clock-and-guards"
    assert proof["sha256"] == hashlib.sha256(raw.encode()).hexdigest()
    assert proof["other_benchmark_globals_shared"]


def test_rebalance_reference_loads_only_one_function_with_canonical_globals(tmp_path):
    import hashlib
    from scripts import benchmark_opening_signal_pipeline as benchmark
    path = tmp_path / "trusted-rebalance.py"
    raw = ("raise RuntimeError('module startup must not execute')\n"
           "def build_rebalance_rows():\n"
           "    return classify_rebalance_action(0.0, 0.2)\n")
    path.write_text(raw)
    function, proof = benchmark._reference_rebalance_function(path)
    assert function.__globals__ is vars(benchmark.portfolio_state)
    assert function() == "BUY"
    assert proof["sha256"] == hashlib.sha256(raw.encode()).hexdigest()
    assert proof["other_benchmark_globals_shared"]
    path.write_text("@unsafe\ndef build_rebalance_rows(): pass\n")
    with pytest.raises(ValueError, match="undecorated"):
        benchmark._reference_rebalance_function(path)


def test_transport_reference_executes_only_selected_method_with_shared_globals(tmp_path):
    import hashlib
    from scripts import benchmark_opening_signal_pipeline as benchmark
    path = tmp_path / "trusted-transport.py"
    raw = ("raise RuntimeError('module startup must not execute')\n"
           "@unsafe\nclass FactorizedLiveWindow(unsafe_base):\n"
           "    raise RuntimeError('class startup must not execute')\n"
           "    def materialize(self, device): return torch.device(device)\n")
    path.write_text(raw)
    method, proof = benchmark._reference_live_materialize(path)
    assert method.__globals__ is vars(benchmark.factorized_panel)
    assert method(None, "cpu") == torch.device("cpu")
    assert proof["sha256"] == hashlib.sha256(raw.encode()).hexdigest()
    assert proof["method"] == "FactorizedLiveWindow.materialize"


@pytest.mark.parametrize("flags,valid", [
    (["--compare-full-shape-prefix", "--preopen-panel-warmup", "--candidate-first"], True),
    (["--compare-full-shape-prefix", "--candidate-first"], False),
    (["--compare-full-shape-prefix", "--preopen-panel-warmup", "--compare-config-reuse"], False),
    (["--candidate-first"], False),
    (["--repeats", "0"], False),
    (["--preopen-quote-priority"], False),
    (["--preopen-quote-priority", "--preopen-panel-warmup"], True),
    (["--preopen-quote-priority", "--preopen-panel-warmup", "--compare-config-reuse"], False),
])
def test_full_shape_benchmark_cli_validates_axes_before_source_io(monkeypatch, flags, valid):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    def preflight(_path):
        raise RuntimeError("valid_axes_reached_preflight_without_source_io")
    monkeypatch.setattr(benchmark, "_benchmark_runtime_profile", preflight)
    monkeypatch.setattr("sys.argv", ["benchmark", "--receipt", "must-not-read.json",
        "--feature-date", "2026-10-21", "--output", "must-not-write.json", *flags])
    if valid:
        with pytest.raises(RuntimeError, match="valid_axes_reached_preflight"):
            benchmark.main()
    else:
        with pytest.raises(SystemExit) as result:
            benchmark.main()
        assert result.value.code == 2


@pytest.mark.parametrize("axis", ["direct_destination", "prepared_host_packets"])
def test_direct_transport_candidate_explicitly_selects_experimental_assembly(axis):
    from scripts.benchmark_opening_signal_pipeline import _experimental_live_materialize
    calls = []
    def materialize(window, device, **kwargs):
        calls.append((window, device, kwargs))
        return "owned-window"
    candidate = _experimental_live_materialize(materialize, axis=axis)
    assert calls == []
    assert candidate("fixture", "cuda") == "owned-window"
    assert calls == [("fixture", "cuda", {axis: True})]
    with pytest.raises(ValueError, match="unknown"):
        _experimental_live_materialize(materialize, axis="not-a-transport-policy")


@pytest.mark.parametrize("raw", (
    "class FactorizedLiveWindow: pass\n",
    "class FactorizedLiveWindow:\n    @unsafe\n    def materialize(self, device): pass\n",
    "class FactorizedLiveWindow:\n    def materialize(self, device): pass\n    def materialize(self, device): pass\n",
    "class FactorizedLiveWindow:\n    def materialize(self, device): pass\nclass FactorizedLiveWindow: pass\n"))
def test_transport_reference_rejects_missing_or_ambiguous_methods(tmp_path, raw):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    path = tmp_path / "invalid-transport.py"
    path.write_text(raw)
    with pytest.raises(ValueError, match="canonical materialize"):
        benchmark._reference_live_materialize(path)


@pytest.mark.parametrize("raw", ("def generate_live_signal(): pass\n", "def generate_live_signal(): pass\ndef generate_live_signal(): pass\ndef _build_decision_rows(): pass\n", "@unsafe\ndef generate_live_signal(): pass\ndef _build_decision_rows(): pass\n"))
def test_row_reference_rejects_ambiguous_or_decorated_definitions(tmp_path, raw):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    path = tmp_path / "bad-release.py"
    path.write_text(raw)
    with pytest.raises(ValueError):
        benchmark._reference_row_functions(path)


def test_explicit_candidate_adds_only_nullable_projection_definition(tmp_path):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    path = tmp_path / "candidate.py"
    path.write_text("def generate_live_signal(): pass\ndef _build_decision_rows(): pass\ndef _finite_float_column(values): return values.tolist()\n")
    functions, proof = benchmark._reference_row_functions(path, nullable_column=True)
    assert sorted(functions) == ["_build_decision_rows", "_finite_float_column", "generate_live_signal"]
    assert proof["functions"] == sorted(functions)
    assert all(function.__globals__ is vars(benchmark.signal_engine) for function in functions.values())


def test_preopen_preparation_never_consumes_opening_quotes_or_writes():
    original = {"price_source": "tw", "write": True, "asof_date": "2026-10-22 09:00:32",
                "panel_date": "2026-10-21", "checkpoint": "same"}
    kwargs = _preopen_kwargs(original, "2026-10-22")
    assert kwargs["price_source"] == "panel" and kwargs["write"] is False
    assert kwargs["asof_date"] == "2026-10-22 08:50:00"
    assert kwargs["checkpoint"] == "same" and kwargs["panel_date"] == "2026-10-21"
    assert original["write"] is True and original["price_source"] == "tw"


def test_preopen_preparation_uses_canonical_private_transport_admission():
    from stockagent.live.market_config import LiveMarketConfig, SIGNAL_UPDATE_PROGRESSIVE
    cfg = LiveMarketConfig(market="fixture", label="fixture", config_path="fixture.yaml",
        day_trade_simulation_enabled=True, day_trade_signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE)
    original = cfg.signal_kwargs(price_source="tw_mis", write=False)
    assert "_prepare_progressive_opening_input" not in original
    warm = _preopen_kwargs(original, "2026-10-22", market_config=cfg)
    assert warm["_prepare_progressive_opening_input"] is True
    assert not warm["write"] and warm["price_source"] == "panel"


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


@pytest.mark.parametrize("damage", [None, "unpersisted_state", "future_financial_field", "fill_fee", "invalid_journal"])
def test_complete_ledger_oracle_reads_all_generated_values_and_only_normalizes_own_root(tmp_path, damage):
    from datetime import datetime
    from scripts.benchmark_opening_signal_pipeline import _paper_ledger_financial_oracle, _new_isolated_entry_engine
    from stockagent.live import tw_day_trade_simulation as simulation
    from stockagent.live.tw_day_trade_service_sync import PAPER_JOURNAL_FILENAMES
    now = datetime.fromisoformat("2026-10-22T09:00:00+08:00")
    engines, proofs = [], []
    for name in ("one", "two"):
        root = tmp_path / name
        engine = _new_isolated_entry_engine(root / "ledger", created_at=now)
        engine.state["modes"]["fixture"] = {"market": "fixture", "future_financial_field": -0.,
            "positions": {"p": {"signed_shares": 1000, "entry_price": 40., "entry_fee": 28.5}},
            "source_path": str(root / "signals/source.json"),
            "outside_source": str(tmp_path / "same-canonical-source"), "integer_clock": 2**63 + 1}
        for filename in PAPER_JOURNAL_FILENAMES:
            simulation._append_jsonl(engine.state_dir / filename, {"symbol": "2330", "shares": 1000,
                "price": 40., "fee": 28.5, "source": str(root / "signals/source.json")})
        engine._persist(now)
        engines.append(engine)
        proofs.append(_paper_ledger_financial_oracle(engine))
    assert proofs[0] == proofs[1]
    assert all(row["records"] == 1 for row in proofs[0]["journals"].values())
    changed = engines[1]
    if damage == "unpersisted_state":
        changed.state["modes"]["fixture"]["positions"]["p"]["entry_fee"] += 1e-12
        with pytest.raises(RuntimeError, match="financial state differs"):
            _paper_ledger_financial_oracle(changed)
    elif damage == "future_financial_field":
        changed.state["modes"]["fixture"]["future_financial_field"] = 0.
        changed._persist(now)
        assert _paper_ledger_financial_oracle(changed)["state_sha256"] != proofs[0]["state_sha256"]
    elif damage == "fill_fee":
        simulation._append_jsonl(changed.fills_path, {"fee": 1e-12})
        changed._persist(now)
        assert _paper_ledger_financial_oracle(changed)["journals"]["fills.jsonl"] != proofs[0]["journals"]["fills.jsonl"]
    elif damage == "invalid_journal":
        changed.events_path.write_text("{broken\n")
        changed._persist(now)
        with pytest.raises(ValueError):
            _paper_ledger_financial_oracle(changed)


def test_isolated_fixture_initial_clock_is_explicit_and_never_normalized_away(tmp_path, monkeypatch):
    from datetime import datetime, timedelta
    from scripts.benchmark_opening_signal_pipeline import _new_isolated_entry_engine, _paper_ledger_financial_oracle
    from stockagent.live import tw_day_trade_simulation as simulation
    fixture_clock = datetime.fromisoformat("2026-10-22T08:50:00+08:00")
    proofs = []
    for index in range(2):
        monkeypatch.setattr(simulation, "_iso", lambda *_: f"2026-10-25T12:00:0{index}+08:00")
        engine = _new_isolated_entry_engine(tmp_path / str(index) / "ledger", created_at=fixture_clock)
        assert engine.state["created_at"] == fixture_clock.isoformat(timespec="seconds")
        engine._persist(fixture_clock)
        proofs.append(_paper_ledger_financial_oracle(engine))
    assert proofs[0] == proofs[1]
    engine.state["created_at"] = (fixture_clock + timedelta(seconds=1)).isoformat(timespec="seconds")
    engine._persist(fixture_clock)
    assert _paper_ledger_financial_oracle(engine)["state_sha256"] != proofs[0]["state_sha256"]


@pytest.mark.parametrize("existing", ["state", "journal", "file"])
def test_isolated_fixture_refuses_any_existing_ledger_content(tmp_path, existing):
    from datetime import datetime
    from scripts.benchmark_opening_signal_pipeline import _new_isolated_entry_engine
    destination = tmp_path / "ledger"
    if existing == "file":
        destination.write_text("existing")
        target = destination
    else:
        destination.mkdir()
        target = destination / ("state.json" if existing == "state" else "fills.jsonl")
        target.write_text("existing")
    with pytest.raises(ValueError, match="empty destination"):
        _new_isolated_entry_engine(destination, created_at=datetime.fromisoformat("2026-10-22T08:50:00+08:00"))
    assert target.read_text() == "existing"


def test_isolated_fixture_refuses_unknown_timezone(tmp_path):
    from datetime import datetime
    from scripts.benchmark_opening_signal_pipeline import _new_isolated_entry_engine
    with pytest.raises(ValueError, match="explicit timezone"):
        _new_isolated_entry_engine(tmp_path / "ledger", created_at=datetime(2026, 10, 22, 8, 50))


@pytest.mark.parametrize("change", ["unchanged", "append", "rewrite_and_append", "truncate",
                                   "replace", "missing", "unterminated", "blank_lines", "empty"])
def test_financial_oracle_prefix_reuse_matches_fresh_full_readback(tmp_path, change):
    from datetime import datetime
    from scripts.benchmark_opening_signal_pipeline import _paper_ledger_financial_oracle
    from stockagent.live import tw_day_trade_simulation as simulation
    from stockagent.live.tw_day_trade_service_sync import PAPER_JOURNAL_FILENAMES
    now = datetime.fromisoformat("2026-10-22T09:00:00+08:00")
    engine = simulation.TwDayTradeSimulationEngine(tmp_path / "run/ledger")
    for name in PAPER_JOURNAL_FILENAMES:
        simulation._append_jsonl_many(engine.state_dir / name, [{"fee": 1.}, {"fee": 2.}])
    if change == "unterminated":
        engine.fills_path.write_bytes(b'{"fee":1.0}')
    elif change == "blank_lines":
        engine.fills_path.write_bytes(b'\n{"fee":1.0}\n \n')
    elif change == "empty":
        engine.fills_path.write_bytes(b"")
    engine._persist(now)
    original = _paper_ledger_financial_oracle(engine)
    if change == "append" or change == "blank_lines" or change == "empty":
        simulation._append_jsonl(engine.fills_path, {"fee": 3.})
    elif change == "rewrite_and_append":
        raw = engine.fills_path.read_bytes().replace(b'1.0', b'9.0')
        engine.fills_path.write_bytes(raw + b'{"fee":3.0}\n')
    elif change == "truncate":
        engine.fills_path.write_bytes(b'{"fee":3.0}\n')
    elif change == "replace":
        replacement = engine.state_dir / "replacement.jsonl"
        replacement.write_bytes(engine.fills_path.read_bytes())
        replacement.replace(engine.fills_path)
    elif change == "missing":
        engine.fills_path.unlink()
    elif change == "unterminated":
        with engine.fills_path.open("ab") as stream:
            stream.write(b'\n{"fee":3.0}\n')
    engine._persist(now)
    incremental = _paper_ledger_financial_oracle(engine)
    work = engine._benchmark_financial_readback_work.copy()
    del engine._benchmark_financial_journal_cache
    assert _paper_ledger_financial_oracle(engine) == incremental
    if change == "unchanged":
        # A second persist advances the real state revision. Only unchanged
        # journals should match; never normalize a financial decision clock.
        assert incremental["journals"] == original["journals"]
        assert incremental["state_sha256"] != original["state_sha256"]
        assert all(item["parsed_records"] == 0 and item["verified_prefix_reused"]
                   and item["verified_old_prefix_bytes"] > 0 for item in work.values())
    elif change in {"append", "blank_lines", "empty"}:
        assert work["fills.jsonl"]["parsed_records"] == 1
        assert work["fills.jsonl"]["verified_prefix_reused"]
    elif change in {"rewrite_and_append", "truncate", "replace", "unterminated"}:
        assert work["fills.jsonl"]["parsed_records"] == incremental["journals"]["fills.jsonl"]["records"]
        assert not work["fills.jsonl"]["verified_prefix_reused"]


@pytest.mark.parametrize("damage", ["corrupt_prefix_with_growth", "non_object", "unpersisted_state"])
def test_financial_oracle_does_not_publish_partial_cache_after_failed_readback(tmp_path, damage):
    from datetime import datetime
    from scripts.benchmark_opening_signal_pipeline import _paper_ledger_financial_oracle
    from stockagent.live import tw_day_trade_simulation as simulation
    now = datetime.fromisoformat("2026-10-22T09:00:00+08:00")
    engine = simulation.TwDayTradeSimulationEngine(tmp_path / "run/ledger")
    simulation._append_jsonl(engine.fills_path, {"fee": 1.})
    engine._persist(now)
    _paper_ledger_financial_oracle(engine)
    before = engine._benchmark_financial_journal_cache
    if damage == "unpersisted_state":
        engine.state["future_financial_field"] = 1e-12
    else:
        engine.fills_path.write_bytes(b'{bad-prefix}\n{"fee":2.0}\n'
                                     if damage == "corrupt_prefix_with_growth" else b'[]\n')
        engine._persist(now)
    with pytest.raises((ValueError, RuntimeError)):
        _paper_ledger_financial_oracle(engine)
    assert engine._benchmark_financial_journal_cache is before


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
        "live_transport_reference": {"path": "trusted-source.py", "sha256": "reference-digest"},
        "preopen_preparation": {"panel_only": True, "opening_quotes_used_for_preparation": False,
            "reference_calls": [{"phase": "opening", "elapsed_ms": 2., "same_reference_object_reused": True}],
            "reference_inventories": [{"prepared_during": "preopen", "source_cutoff": "2026-10-21",
                "symbols": ["A", "B", "C"], "prices": [100., None, 0.],
                "source_dates": ["2026-10-20", None, None]}]},
        "samples": [{"variant": "candidate", "sample_phase": "first_opening_after_panel_only_preparation",
            "all_published_ms": 100., "all_artifacts_ms": 120., "markets": [{"market": "mode",
                "cpu_clock_contract": "same_thread_cpu_or_unknown_v1",
                "input_preparation_owner_thread_cpu_ms": {"panel_join": None, "quote": 1.5},
                "stages": {"model_inference_ms": 2., "source_scope": ["keep-in-full-file"],
                           "pre_quote_local_prepare_steps_ms": {"panel_source_and_alignment": 0.7},
                           "post_quote_prepare_steps_ms": {"previous_account_projection": 0.3,
                                                           "opening_input_coverage": None},
                           "factorized_input_transport": {"destination_policy": "owned_full_window_slices_v1",
                                                          "avoided_dense_copy_bytes": 1000}},
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
    assert result["live_transport_reference"] == evidence["live_transport_reference"]
    assert result["native_opening_to_completion_claim"] is False
    assert result["preopen_preparation"]["reference_inventories"][0]["valid_reference_count"] == 1
    assert result["preopen_preparation"]["reference_calls"]["opening"]["same_reference_object_reused_count"] == 1
    sample = result["samples"][0]
    assert sample["sample_phase"] == "first_opening_after_panel_only_preparation"
    market = sample["markets"][0]
    assert market["cpu_clock_contract"] == "same_thread_cpu_or_unknown_v1"
    assert market["input_preparation_owner_thread_cpu_ms"] == {"panel_join": None, "quote": 1.5}
    assert market["stages"] == {"model_inference_ms": 2.,
        "pre_quote_local_prepare_steps_ms": {"panel_source_and_alignment": 0.7},
        "post_quote_prepare_steps_ms": {"previous_account_projection": 0.3,
                                         "opening_input_coverage": None}}
    assert market["factorized_input_transport"] == {"destination_policy": "owned_full_window_slices_v1",
                                                    "avoided_dense_copy_bytes": 1000}
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


@pytest.mark.parametrize("failed", [False, True])
def test_benchmark_separates_owner_thread_cpu_and_keeps_original_callbacks(monkeypatch, failed):
    from scripts import benchmark_opening_signal_pipeline as benchmark
    elapsed = iter([1., 2., 2.5, 3.])
    cpu = iter([0., .1, .3, .4])
    monkeypatch.setattr(benchmark.time, "perf_counter", lambda: next(elapsed))
    monkeypatch.setattr(benchmark.time, "thread_time", lambda: next(cpu))
    events = []
    def original(**kwargs):
        assert kwargs["input"] == "unchanged"
        for phase in ("prepare", "forward", "forward"):
            kwargs["progress_callback"]({"message": phase, "step": 1})
        if failed:
            raise ValueError("original failure")
        return "original result"
    inputs = {"input": "unchanged", "progress_callback": events.append}
    wall_stages, cpu_stages = {}, {}
    if failed:
        with pytest.raises(ValueError, match="original failure"):
            benchmark._call_with_stage_timing(original, inputs, wall_stages, cpu_stages)
    else:
        assert benchmark._call_with_stage_timing(original, inputs, wall_stages, cpu_stages) == "original result"
    assert wall_stages == {"prepare": 500., "forward": 500.}
    assert cpu_stages == pytest.approx({"prepare": 200., "forward": 100.})
    assert events == [{"message": phase, "step": 1} for phase in ("prepare", "forward", "forward")]
    assert inputs["input"] == "unchanged" and inputs["progress_callback"] == events.append


@pytest.mark.parametrize("candidate", [False, True])
@pytest.mark.parametrize("comparison", ["config_reuse", "validation", "other"])
def test_config_and_schema_comparisons_change_only_the_selected_axis(candidate, comparison):
    from scripts.benchmark_opening_signal_pipeline import _config_variant_loaders
    cached, full, raw_schema = object(), object(), object()
    schema = SimpleNamespace(__wrapped__=raw_schema)
    loader, validator = _config_variant_loaders(cached, full, schema, use_cached=candidate,
        compare_config_reuse=comparison == "config_reuse", compare_validation=comparison == "validation",
        legacy_validation_control=False)
    if comparison == "config_reuse":
        assert loader is (cached if candidate else full)
        assert validator is schema  # Never disable the pre-existing schema cache.
    elif comparison == "validation":
        assert loader is full  # Both sides must still execute full parsing.
        assert validator is (schema if candidate else raw_schema)
    else:
        assert loader is cached and validator is schema

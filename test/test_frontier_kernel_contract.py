from copy import deepcopy
from dataclasses import replace
import sys
import warnings
from types import ModuleType
from types import SimpleNamespace
from contextlib import nullcontext
import pytest
import torch
from torch import nn

from stockagent.config import (
    DataConfig, EnvironmentConfig, EvaluationConfig, ExperimentConfig,
    RunnerConfig, TradingConfig, TrainingConfig, WalkForwardConfig,
)
from stockagent.models.financial_transformer import CandleEncoder
from stockagent.models.kernel_backends import _adopt
from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot


def _config():
    return ExperimentConfig(experiment_name="frontier-contract", runner=RunnerConfig(),
        environment=EnvironmentConfig(device="cpu", use_tensor_cores=True, amp_dtype="bf16"),
        data=DataConfig(parquet_root="fixture", benchmark_name="fixture"),
        walk_forward=WalkForwardConfig(), trading=TradingConfig(
            frequency="daily", buy_fee_rate=.000855, sell_fee_rate=.003855, long_only=False),
        training=TrainingConfig(model_name="financial_transformer", non_blocking_transfer=True),
        evaluation=EvaluationConfig())


def test_graph_capture_inspection_preserves_native_and_unknown_evidence(monkeypatch):
    from stockagent.training.trainer import _cuda_graph_capture_snapshot
    config = _config()
    def unavailable():
        raise RuntimeError("CUDA inspection unavailable")
    monkeypatch.setattr(torch.cuda, "current_device", unavailable)
    assert _cuda_graph_capture_snapshot(config) == {}
    config.training.financial_transformer.factorized_portfolio_cuda_graphs = True
    observed = _cuda_graph_capture_snapshot(config)
    assert observed["cuda_graph_root_count_local"] is None
    assert observed["cuda_graph_node_count_local"] is None
    assert observed["cuda_graph_runtime_inspection_ok"] is False


def test_graph_capture_inspection_counts_actual_root_nodes(monkeypatch):
    from stockagent.training.trainer import _cuda_graph_capture_snapshot
    import torch._inductor.cudagraph_trees as trees
    config = _config()
    config.training.financial_transformer.factorized_portfolio_cuda_graphs = True
    monkeypatch.setattr(torch.cuda, "current_device", lambda: 1)
    root = SimpleNamespace(children={0: [object()]})
    monkeypatch.setattr(trees, "get_container", lambda device: SimpleNamespace(
        tree_manager=SimpleNamespace(roots={0: [root, object()], 1: [object()]})))
    observed = _cuda_graph_capture_snapshot(config)
    assert observed == {"cuda_graph_root_count_local": 3, "cuda_graph_node_count_local": 4,
        "cuda_graph_runtime_inspection_ok": True, "cuda_graph_device_index_local": 1}


def test_native_defaults_do_not_change_semantic_checkpoint_snapshot():
    snapshot = _configuration_fingerprint_snapshot(_config())
    model = snapshot["training"]["financial_transformer"]
    assert not any(key.startswith("kernel_") for key in model)
    assert "compact_projection_compute_dtype" not in model
    assert "factorized_compile_autotune" not in model
    assert "factorized_cuda_graphs" not in model
    assert "kernel_package_versions" not in model
    assert "ddp_broadcast_buffers" not in snapshot["training"]
    assert "ddp_trajectory_gradient_sync" not in snapshot["training"]


def test_flash4_custom_op_contract_versions_resume_without_changing_execution():
    native = _config()
    candidate = deepcopy(native)
    candidate.training.financial_transformer.kernel_attention_backend = "flash4_temporal"
    old, new = map(_configuration_fingerprint_snapshot, (native, candidate))
    assert new["training"]["financial_transformer"]["flash4_unmasked_contract"] == "vendor_forward_backward_custom_op_v2"
    assert "flash4_unmasked_contract" not in old["training"]["financial_transformer"]
    for section in ("data", "trading", "walk_forward", "evaluation"):
        assert new[section] == old[section]


@pytest.mark.parametrize("backend,contract,version", [
    ("sdpa_explicit", "sdpa_dispatch_contract", "masked_credit_math_only_v2"),
    ("sdpa_padded", "sdpa_dispatch_contract", "masked_credit_aligned_key128_channel16_excludes_cudnn_v1"),
    ("manual_fp32_last", "last_query_mask_contract", "empty_key_mask_zero_credit_v1"),
])
def test_explicit_sdpa_cannot_resume_legacy_workaround_checkpoint(backend, contract, version):
    legacy = _config()
    legacy.training.financial_transformer.compiled_cross_attention_backend = "sdpa"
    explicit = deepcopy(legacy)
    explicit.training.financial_transformer.compiled_cross_attention_backend = backend
    assert _configuration_fingerprint_snapshot(legacy) != _configuration_fingerprint_snapshot(explicit)
    assert _configuration_fingerprint_snapshot(explicit)["training"]["financial_transformer"][
        contract] == version


@pytest.mark.parametrize("field,value", [
    ("ddp_broadcast_buffers", False), ("ddp_trajectory_gradient_sync", "trajectory"),
])
def test_ddp_recipe_versions_resume_without_changing_data_or_execution(field, value):
    native = _config()
    candidate = replace(native, training=replace(native.training, **{field: value}))
    old, new = map(_configuration_fingerprint_snapshot, (native, candidate))
    assert new["training"] != old["training"]
    for section in ("data", "trading", "walk_forward", "evaluation"):
        assert new[section] == old[section]


def test_optional_kernel_package_upgrade_invalidates_optimizer_resume(monkeypatch):
    import stockagent.training.checkpoint_contract as contracts
    native = _config()
    candidate = replace(native, training=replace(native.training, financial_transformer=replace(
        native.training.financial_transformer, kernel_linear_backend="torchao_fp8")))
    monkeypatch.setattr(contracts.importlib.metadata, "version", lambda package: "1")
    old = _configuration_fingerprint_snapshot(candidate)
    assert old["training"]["financial_transformer"]["kernel_package_versions"] == {"torchao": "1", "triton": "1"}
    assert old["training"]["financial_transformer"]["torchao_padding_contract"] == "neutral_mnk16_crop_full_credit_v2"
    monkeypatch.setattr(contracts.importlib.metadata, "version", lambda package: "2")
    new = _configuration_fingerprint_snapshot(candidate)
    assert old["training"] != new["training"] and old["data"] == new["data"]
    assert _configuration_fingerprint_snapshot(native)["training"].get("kernel_package_versions") is None
    assert "torchao_padding_contract" not in _configuration_fingerprint_snapshot(native)["training"]["financial_transformer"]


def test_precision_and_backend_changes_version_resume_while_preserving_execution():
    original = _config()
    base = _configuration_fingerprint_snapshot(original)
    for key, value in {"kernel_norm_backend": "liger", "kernel_linear_backend": "quack",
            "kernel_swiglu_backend": "liger", "kernel_attention_backend": "flash4",
            "kernel_softmax_backend": "quack",
            "compact_projection_compute_dtype": "bf16", "factorized_compile_autotune": True,
            "factorized_cuda_graphs": True, "factorized_relaxed_precision_casts": True,
            "factorized_encoder_force_retention": True, "factorized_encoder_packet_cache": False,
            "fused_swiglu_projection": True}.items():
        model = replace(original.training.financial_transformer, **{key: value})
        candidate = replace(original, training=replace(original.training, financial_transformer=model))
        snapshot = _configuration_fingerprint_snapshot(candidate)
        assert snapshot["training"] != base["training"]
        for section in ("data", "trading", "walk_forward", "evaluation"):
            assert snapshot[section] == base[section]


def test_adapters_retain_parameter_identity_checkpoint_names_and_rng():
    original = nn.LayerNorm(19)
    state = torch.random.get_rng_state().clone()
    adapter = _adopt(original, nn.LayerNorm, None)
    assert adapter.weight is original.weight and adapter.bias is original.bias
    assert tuple(adapter.state_dict()) == tuple(original.state_dict())
    assert torch.equal(state, torch.random.get_rng_state())
    x = torch.randn(3, 19)
    torch.testing.assert_close(adapter(x), original(x), rtol=0, atol=0)


def test_deepgemm_functional_adjoint_keeps_nonleaf_credit_and_fp32_masters(monkeypatch):
    from stockagent.models.kernel_backends import deepgemm_bf16_function
    package = ModuleType("deep_gemm")
    def gemm(a, b, out):
        out.copy_((a.float() @ b.float().t()).to(out.dtype))
    package.bf16_gemm_nt = gemm
    monkeypatch.setitem(sys.modules, "deep_gemm", package)
    torch.manual_seed(91)
    x = torch.randn(3, 5, 8, requires_grad=True)
    master = nn.Parameter(torch.randn(16, 19))
    ids = torch.tensor([0, 2, 3, 7, 10, 12, 15, 18])
    weight = master.index_select(1, ids)
    bias = nn.Parameter(torch.randn(16))
    out = deepgemm_bf16_function()(x, weight, bias)
    expected = (x.reshape(-1, 8).bfloat16().float() @ weight.bfloat16().float().t()).bfloat16().float()
    torch.testing.assert_close(out, expected.reshape(3, 5, 16) + bias, rtol=0, atol=0)
    coefficients = torch.randn_like(out)
    (out * coefficients).sum().backward()
    g = coefficients.reshape(-1, 16).bfloat16().float()
    torch.testing.assert_close(x.grad, (g @ weight.bfloat16().float()).reshape_as(x), rtol=0, atol=0)
    selected_grad = g.t() @ x.reshape(-1, 8).bfloat16().float()
    torch.testing.assert_close(master.grad[:, ids], selected_grad, rtol=0, atol=0)
    assert master.grad.dtype == master.dtype == torch.float32
    assert master.grad[:, 1].count_nonzero() == 0
    torch.testing.assert_close(bias.grad, coefficients.sum((0, 1)), rtol=1e-6, atol=1e-6)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a real CUDA device")
def test_deepgemm_fullgraph_preserves_vendor_calls_and_nonleaf_column_credit(monkeypatch):
    from stockagent.models.kernel_backends import deepgemm_bf16_function
    from torch._dynamo.utils import counters
    package = ModuleType("deep_gemm")
    calls = []
    def gemm(a, b, out):
        calls.append((a.shape, b.shape, out.dtype))
        out.copy_((a.float() @ b.float().t()).to(out.dtype))
    package.bf16_gemm_nt = gemm
    monkeypatch.setitem(sys.modules, "deep_gemm", package)
    linear = deepgemm_bf16_function()
    inputs = (torch.randn(3, 5, 8, device="cuda", requires_grad=True),
        torch.randn(16, 19, device="cuda", requires_grad=True),
        torch.randn(16, device="cuda", requires_grad=True))
    reference = tuple(x.detach().clone().requires_grad_() for x in inputs)
    ids = torch.tensor([0, 2, 3, 7, 10, 12, 15, 18], device="cuda")
    expected = linear(reference[0], reference[1].index_select(1, ids), reference[2])
    coefficients = torch.randn_like(expected)
    expected_grads = torch.autograd.grad(expected, reference, coefficients)
    begin = counters["stats"]["unique_graphs"]
    class Stem(nn.Module):
        def __init__(self):
            super().__init__()
            self._compact_linear = linear

        def forward(self, x, weight, bias):
            kernel = getattr(self, "_compact_linear", torch.nn.functional.linear)
            return kernel(x, weight, bias)

    compiled = torch.compile(Stem(), fullgraph=True, options={"triton.cudagraphs": False})
    for _ in range(2):
        # Pytest promotes warnings to errors. Dynamo's own metadata/context
        # probes trigger these two warnings; no adapter instantiates Function.
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning,
                message=r"The \.grad attribute of a Tensor that is not a leaf Tensor.*")
            warnings.filterwarnings("ignore", category=DeprecationWarning,
                message=r"<class 'torch\.autograd\.function\.Function'> should not be instantiated.*")
            actual = compiled(inputs[0], inputs[1].index_select(1, ids), inputs[2])
        grads = torch.autograd.grad(actual, inputs, coefficients)
        torch.testing.assert_close(actual, expected, rtol=0, atol=0)
        for a, b in zip(grads, expected_grads, strict=True):
            torch.testing.assert_close(a, b, rtol=1e-6, atol=1e-6)
        assert grads[1][:, 1].count_nonzero() == 0
        assert grads[1].dtype == inputs[1].dtype == torch.float32
    assert counters["stats"]["unique_graphs"] - begin == 1
    assert len(calls) >= 9  # eager plus two compiled forward/backward calls
    assert any(dtype == torch.float32 for _, _, dtype in calls)


@pytest.mark.parametrize("input_shape,outputs", [((7, 8), 16), ((2, 7, 8), 88), ((7, 88), 88)])
def test_torchao_functional_adapter_reuses_official_autograd_without_parameter_detach(monkeypatch, input_shape, outputs):
    from stockagent.models.kernel_backends import torchao_fp8_function
    package, module, core = ModuleType("torchao"), ModuleType("torchao.float8"), ModuleType("torchao.float8.float8_linear")
    package.__path__, module.__path__ = [], []
    def config(**kwargs):
        return SimpleNamespace(emulate=False, pad_inner_dim=kwargs["pad_inner_dim"],
            gemm_config_output=SimpleNamespace(use_fast_accum=True),
            gemm_config_grad_input=SimpleNamespace(use_fast_accum=False),
            gemm_config_grad_weight=SimpleNamespace(use_fast_accum=False))
    module.Float8LinearConfig = config
    core.ScaledMMConfig, core.LinearMMConfig = lambda *args: args, lambda *args: args
    calls = []
    def apply(x, wt, mm, config):
        assert mm == ((False, True, False, True), (False, False, False, True), (False, False, False, True))
        assert config.pad_inner_dim and wt.requires_grad
        assert x.shape[0] % 16 == x.shape[1] % 16 == wt.shape[1] % 16 == 0
        calls.append(True)
        return x @ wt
    core.matmul_with_hp_or_float8_args = SimpleNamespace(apply=apply)
    for name, value in (("torchao", package), ("torchao.float8", module), ("torchao.float8.float8_linear", core)):
        monkeypatch.setitem(sys.modules, name, value)
    before = torch.random.get_rng_state().clone()
    linear = torchao_fp8_function()
    assert torch.equal(before, torch.random.get_rng_state())
    width = input_shape[-1]
    x = torch.randn(*input_shape, requires_grad=True)
    master = nn.Parameter(torch.randn(outputs, width + 3))
    bias = nn.Parameter(torch.randn(outputs))
    ids = torch.cat((torch.tensor([0]), torch.arange(2, width + 1)))
    out = linear(x, master.index_select(1, ids), bias)
    expected = torch.nn.functional.linear(x, master.index_select(1, ids), bias)
    torch.testing.assert_close(out, expected)
    reference = torch.autograd.grad(expected.sum(), (x, master, bias))
    out.sum().backward()
    assert calls and master.grad.dtype == torch.float32
    for actual, expected_grad in zip((x.grad, master.grad, bias.grad), reference, strict=True):
        torch.testing.assert_close(actual, expected_grad)
    torch.testing.assert_close(master.grad[:, ids], x.reshape(-1, width).sum(0).expand(outputs, width))
    assert master.grad[:, 1].count_nonzero() == 0


def _mock_transformer_engine(monkeypatch):
    import stockagent.models.kernel_backends as backends
    package, te, common, recipes = (ModuleType(name) for name in
        ("transformer_engine", "transformer_engine.pytorch", "transformer_engine.common", "transformer_engine.common.recipe"))
    package.__path__, common.__path__ = [], []
    class Linear(nn.Linear):
        def __init__(self, n, k, *, bias=True, params_dtype=None, device=None):
            super().__init__(n, k, bias=bias, dtype=params_dtype, device=device)
    te.Linear = Linear
    te.autocast = lambda **kwargs: nullcontext()
    recipes.Float8CurrentScaling, recipes.NVFP4BlockScaling = lambda: None, lambda **kwargs: None
    for name, module in (("transformer_engine", package), ("transformer_engine.pytorch", te),
        ("transformer_engine.common", common), ("transformer_engine.common.recipe", recipes)):
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(backends, "flash4_function", lambda: None)
    monkeypatch.setattr(backends.importlib.metadata, "version", lambda package: "mock-api-only")
    return backends


def test_transformer_engine_adoption_keeps_original_fp32_parameters_and_rng(monkeypatch):
    backends = _mock_transformer_engine(monkeypatch)
    model = nn.Module()
    model.candle_encoder = CandleEncoder(num_features=19, d_model=32, dropout=0., norm_type="layernorm", ffn_type="swiglu", sanitize_inputs=False)
    model.factorized_compact_projection = True
    model.aligned, model.unaligned = nn.Linear(32, 32), nn.Linear(32, 88)
    original = dict(model.named_parameters())
    before = torch.random.get_rng_state().clone()
    settings = replace(_config().training.financial_transformer, kernel_linear_backend="te_fp8")
    receipt = backends.configure_model_kernels(model, settings)
    assert torch.equal(before, torch.random.get_rng_state())
    assert all(dict(model.named_parameters())[name] is parameter for name, parameter in original.items())
    assert all(parameter.dtype == torch.float32 for parameter in model.parameters())
    assert "aligned" in receipt["applied_sites"]["linear"]
    assert "unaligned" in receipt["native_unaligned_linear_sites"]


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a real CUDA device")
def test_transformer_engine_zero_row_padding_crops_outputs_and_entire_credit(monkeypatch):
    backends = _mock_transformer_engine(monkeypatch)
    layer = backends.transformer_engine_linear_class("te_fp8")(32, 32, params_dtype=torch.float32, device="cuda")
    x = torch.randn(3, 5, 32, device="cuda", requires_grad=True)
    actual = layer(x)
    with torch.autocast("cuda", dtype=torch.bfloat16):
        expected = torch.nn.functional.linear(x, layer.weight, layer.bias).float()
    torch.testing.assert_close(actual, expected, rtol=.02, atol=.02)
    coefficients = torch.randn_like(actual)
    reference = torch.autograd.grad(expected, (x, layer.weight, layer.bias), coefficients, retain_graph=True)
    observed = torch.autograd.grad(actual, (x, layer.weight, layer.bias), coefficients)
    for a, b in zip(observed, reference, strict=True):
        assert torch.nn.functional.cosine_similarity(a.flatten(), b.flatten(), dim=0) > .99


def test_physical_fifo_compile_tuning_versions_only_active_nondefault_contract(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_MAX_FUSION_SIZE", "8")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_DISABLE_COALESCE_TILING", "1")
    ordinary = _config()
    selected = replace(ordinary, trading=replace(ordinary.trading, execution_mode="tw_day_trade"))
    ordinary_base = _configuration_fingerprint_snapshot(ordinary)
    native = _configuration_fingerprint_snapshot(selected)
    assert "physical_fifo_compile_contract" not in native["training"]
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_MAX_FUSION_SIZE", "16")
    changed = _configuration_fingerprint_snapshot(selected)
    assert changed["training"]["physical_fifo_compile_contract"]["options"]["max_fusion_size"] == 16
    assert changed["data"] == native["data"] and changed["trading"] == native["trading"]
    assert _configuration_fingerprint_snapshot(ordinary) == ordinary_base
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_MAX_FUSION_SIZE", "8")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_DISABLE_COALESCE_TILING", "0")
    tiled = _configuration_fingerprint_snapshot(selected)
    assert tiled["training"] != native["training"]


def test_calendar_recipe_owns_a_distinct_physical_checkpoint_contract(monkeypatch):
    for flag in ("STOCKAGENT_DAY_TRADE_CALENDAR_CACHE", "STOCKAGENT_DAY_TRADE_CALENDAR_COMPILE",
                 "STOCKAGENT_DAY_TRADE_PREPARE_COMPILE"):
        monkeypatch.setenv(flag, "0")
    native = _config()
    selected = replace(native, trading=replace(native.trading, execution_mode="tw_day_trade"))
    old = _configuration_fingerprint_snapshot(selected)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CALENDAR_CACHE", "1")
    new = _configuration_fingerprint_snapshot(selected)
    assert new["training"] != old["training"]
    assert new["trading"] == old["trading"] and new["data"] == old["data"]
    assert _configuration_fingerprint_snapshot(native) == _configuration_fingerprint_snapshot(_config())
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_PREPARE_COMPILE", "1")
    prepared = _configuration_fingerprint_snapshot(selected)
    assert prepared["training"]["physical_fifo_prepare_contract"] == "tensor_gregorian_bucketed_inventory_funding_preparation_v3"
    assert prepared["trading"] == old["trading"] and prepared["data"] == old["data"]


def test_compact_bf16_stem_preserves_all_affine_columns_and_fp32_master_gradients():
    torch.manual_seed(73)
    model = CandleEncoder(num_features=19, d_model=8, dropout=0., norm_type="layernorm",
        ffn_type="swiglu", projection_fp32=True, sanitize_inputs=False)
    with torch.no_grad():
        model.input_norm.weight.copy_(torch.randn(19))
        model.input_norm.bias.copy_(torch.randn(19))
    reference = deepcopy(model)
    model.compact_projection_compute_dtype = "bf16"
    ids = torch.tensor([1, 3, 10, 17])
    values = torch.randn(5, 3, 4, requires_grad=True)
    control = values.detach().clone().requires_grad_()
    actual = model.forward_compact_columns(values, ids)
    expected = reference.forward_compact_columns(control, ids)
    torch.testing.assert_close(actual, expected, rtol=.025, atol=.015)
    coefficients = torch.randn_like(actual)
    (actual * coefficients).sum().backward()
    (expected * coefficients).sum().backward()
    assert torch.isfinite(values.grad).all() and values.grad.abs().sum() > 0
    # BF16 rounding before subsequent normalization can amplify individual
    # small gradients. Check the overall credit direction, not bit parity.
    cosine = torch.nn.functional.cosine_similarity(values.grad.flatten(), control.grad.flatten(), dim=0)
    assert cosine > .98
    assert .8 < values.grad.norm() / control.grad.norm() < 1.2
    for name, parameter in model.named_parameters():
        assert parameter.dtype == torch.float32
        if parameter.grad is not None:
            assert parameter.grad.dtype == torch.float32 and torch.isfinite(parameter.grad).all()
    # Omitted zero-valued inputs still receive their learned beta contribution.
    assert model.input_norm.bias.grad[0].abs() > 0


@pytest.mark.parametrize("with_bias", [True, False])
def test_quack_layernorm_public_adjoint_preserves_shape_affine_and_fp32_stats(monkeypatch, with_bias):
    from stockagent.models.kernel_backends import quack_layernorm_function
    package = ModuleType("quack")
    package.__path__ = []
    module = ModuleType("quack.rmsnorm")
    def fwd(x, w, b, eps, *, return_rstd, return_mean):
        assert x.ndim == 2 and x.is_contiguous() and return_rstd and return_mean
        out, mean, rstd = torch.native_layer_norm(x, (x.shape[-1],), w, b, eps)
        return out, rstd.flatten(), mean.flatten()
    def bwd(x, w, dout, rstd, mean, *, has_bias):
        assert rstd.dtype == mean.dtype == torch.float32 and dout.is_contiguous()
        b = torch.zeros_like(w) if has_bias else None
        return torch.ops.aten.native_layer_norm_backward(dout, x, (x.shape[-1],),
            mean[:, None], rstd[:, None], w, b, [True, True, has_bias])
    module.layernorm_fwd, module.layernorm_bwd = fwd, bwd
    monkeypatch.setitem(sys.modules, "quack", package)
    monkeypatch.setitem(sys.modules, "quack.rmsnorm", module)
    torch.manual_seed(12)
    x = torch.randn(3, 4, 11)[:, ::2, :].requires_grad_()
    w = torch.randn(11, requires_grad=True)
    b = torch.randn(11, requires_grad=True) if with_bias else None
    inputs = (x, w, b) if with_bias else (x, w)
    actual = quack_layernorm_function()(x, w, b, 1e-5)
    expected = torch.nn.functional.layer_norm(x, (11,), w, b, 1e-5)
    coef = torch.randn_like(actual)
    torch.testing.assert_close(actual, expected)
    grads = torch.autograd.grad(actual, inputs, coef)
    reference = torch.autograd.grad(expected, inputs, coef)
    for a, e in zip(grads, reference, strict=True):
        torch.testing.assert_close(a, e)


def test_packed_swiglu_preserves_parameters_rng_and_all_credit_paths():
    from stockagent.models.transformer_base_portfolio import SwiGLUFeedForward
    torch.manual_seed(18)
    native = SwiGLUFeedForward(32, 88, 0.)
    candidate = deepcopy(native)
    before = torch.get_rng_state().clone()
    candidate.fused_gate_value_projection = True
    assert torch.equal(before, torch.get_rng_state())
    assert tuple(native.state_dict()) == tuple(candidate.state_dict())
    x = torch.randn(3, 5, 32, requires_grad=True)
    control = x.detach().clone().requires_grad_()
    expected, actual = native(control), candidate(x)
    coef = torch.randn_like(actual)
    torch.testing.assert_close(actual, expected)
    actual_grads = torch.autograd.grad(actual, (x, *candidate.parameters()), coef)
    expected_grads = torch.autograd.grad(expected, (control, *native.parameters()), coef)
    for a, e in zip(actual_grads, expected_grads, strict=True):
        torch.testing.assert_close(a, e)

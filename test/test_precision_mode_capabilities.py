import torch
import pytest

from scripts.benchmark_precision_modes import _forward_loss, _make_batch, _make_model, precision_plan
from types import SimpleNamespace
from stockagent.training.trainer import _resolve_amp_dtype


def test_project_amp_contract_remains_bf16_and_fp16_only() -> None:
    assert _resolve_amp_dtype("bf16") is torch.bfloat16
    assert _resolve_amp_dtype("fp16") is torch.float16
    assert _resolve_amp_dtype("tf32") is None


def test_precision_benchmark_runnable_training_modes() -> None:
    expected = {
        "fp32": (True, None, False, False, True, None),
        "tf32": (True, None, True, False, True, None),
        "fp16": (True, torch.float16, True, True, True, None),
        "bf16": (True, torch.bfloat16, True, False, True, None),
        "fp8": (True, torch.bfloat16, True, False, False, "transformer_engine_fp8"),
        "fp4": (True, torch.bfloat16, True, False, False, "transformer_engine_nvfp4"),
        "nf4": (False, None, True, False, False, None),
        "int8": (False, None, True, False, False, None),
    }
    for mode, values in expected.items():
        plan = precision_plan(mode)
        assert (
            plan.runnable,
            plan.autocast_dtype,
            plan.allow_tf32,
            plan.use_grad_scaler,
            plan.native_amp,
            plan.native_backend,
        ) == values


def test_precision_benchmark_uses_native_low_bit_backends_only() -> None:
    for mode in ["fp8", "fp4"]:
        plan = precision_plan(mode)
        assert plan.runnable is True
        assert plan.native_amp is False
        assert plan.native_backend is not None
        assert plan.use_grad_scaler is False
        assert plan.reason
    for mode in ["nf4", "int8"]:
        plan = precision_plan(mode)
        assert plan.runnable is False
        assert plan.native_backend is None
        assert "native" in plan.reason.lower() or "training" in plan.reason.lower()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA AMP smoke requires CUDA")
def test_transformer_base_fp16_amp_mask_path_does_not_overflow() -> None:
    args = SimpleNamespace(
        batch_size=2,
        lookback=8,
        symbols=16,
        features=16,
        seed=2026,
        d_model=32,
        attention_mode="market_token",
        sdpa_batch_limit=16384,
        temporal_layers=1,
        heads=4,
        temporal_pooling="last",
    )
    device = torch.device("cuda")
    batch = _make_batch(args, device)
    batch["tradable_mask"][0, -2:] = False
    batch["can_buy_mask"] = batch["tradable_mask"].clone()
    batch["can_sell_mask"] = batch["tradable_mask"].clone()
    model = _make_model(args, device).train()
    plan = precision_plan("fp16")

    loss, weights, _ = _forward_loss(
        model=model,
        batch=batch,
        device=device,
        plan=plan,
        initial_weights=None,
    )

    assert torch.isfinite(loss.detach()).item()
    assert torch.isfinite(weights.detach()).all().item()
    assert weights[0, -2:].detach().abs().max().item() < 1e-6

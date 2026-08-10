from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest
import torch
from torch import nn

from stockagent.training.runtime import (
    autocast_context,
    call_model,
    extract_weights_and_aux,
    load_checkpoint,
    load_model_state_dict,
    resolve_amp_dtype,
    resolve_device,
    unwrap_model,
)


class _AuxModel(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.seen_mask: torch.Tensor | None = None

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor,
        *,
        return_aux: bool = False,
    ) -> torch.Tensor | dict[str, torch.Tensor]:
        self.seen_mask = mask.detach().clone()
        weights = x[..., 0].masked_fill(~mask, 0.0)
        if return_aux:
            return {"weights": weights, "score_logits": weights + 1.0}
        return weights


class _CompileLikeWrapper(nn.Module):
    def __init__(self, model: nn.Module) -> None:
        super().__init__()
        self._orig_mod = model


def test_shared_runtime_preserves_model_only_empty_row_visibility() -> None:
    model = _AuxModel()
    x = torch.arange(8, dtype=torch.float32).reshape(2, 4, 1)
    trading_mask = torch.tensor(
        [[False, False, False, False], [False, True, False, False]]
    )

    output = call_model(model, x, trading_mask, return_aux=True)
    weights, aux = extract_weights_and_aux(output)

    assert model.seen_mask is not None
    assert model.seen_mask.tolist() == [
        [True, False, False, False],
        [False, True, False, False],
    ]
    assert trading_mask.tolist() == [
        [False, False, False, False],
        [False, True, False, False],
    ]
    assert aux is output
    assert weights.shape == (2, 4)


def test_shared_runtime_loads_nested_wrapper_state_strictly() -> None:
    source = nn.Linear(3, 2)
    target = nn.Linear(3, 2)
    wrapped_state = {
        f"module._orig_mod.{name}": tensor.detach().clone()
        for name, tensor in source.state_dict().items()
    }

    load_model_state_dict(_CompileLikeWrapper(target), wrapped_state)

    assert unwrap_model(_CompileLikeWrapper(target)) is target
    for name, tensor in source.state_dict().items():
        assert torch.equal(target.state_dict()[name], tensor)


def test_shared_checkpoint_loader_rejects_non_mapping_payload(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.pt"
    torch.save({"epoch": 3, "model_state_dict": {}}, mapping_path)
    assert load_checkpoint(mapping_path)["epoch"] == 3

    sequence_path = tmp_path / "sequence.pt"
    torch.save([1, 2, 3], sequence_path)
    with pytest.raises(TypeError, match="must contain a mapping"):
        load_checkpoint(sequence_path)


def test_trainer_reuses_shared_runtime_implementations() -> None:
    from stockagent.training import trainer

    assert trainer._autocast_context is autocast_context
    assert trainer._call_model is call_model
    assert trainer._extract_weights_and_aux is extract_weights_and_aux
    assert trainer._load_checkpoint is load_checkpoint
    assert trainer._resolve_amp_dtype is resolve_amp_dtype
    assert trainer._resolve_device is resolve_device
    assert trainer._unwrap_model is unwrap_model


def test_live_module_import_does_not_eagerly_load_trainer() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; import stockagent.live.signal_engine; "
                "print('stockagent.training.trainer' in sys.modules)"
            ),
        ],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "False"


def test_shared_precision_contract() -> None:
    assert resolve_amp_dtype("bf16") is torch.bfloat16
    assert resolve_amp_dtype("fp16") is torch.float16
    assert resolve_amp_dtype("tf32") is None
    with pytest.raises(ValueError, match="Unsupported amp dtype"):
        resolve_amp_dtype("fp8")

    config = type(
        "Config",
        (),
        {"environment": type("Environment", (), {"device": "cpu"})()},
    )()
    assert resolve_device(config) == torch.device("cpu")
    with autocast_context(torch.device("cpu"), torch.bfloat16):
        result = torch.ones(1) + 1
    assert result.dtype is torch.float32

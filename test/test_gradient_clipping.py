import torch

from stockagent.training import trainer


def test_fast_gradient_clip_prefers_foreach_without_nonfinite_sync(monkeypatch) -> None:
    model = torch.nn.Linear(4, 3)
    loss = model(torch.ones(2, 4)).sum()
    loss.backward()

    calls: list[dict[str, object]] = []
    original_clip = torch.nn.utils.clip_grad_norm_

    def wrapped_clip(parameters, *args, **kwargs):
        params = list(parameters)
        calls.append(
            {
                "error_if_nonfinite": kwargs.get("error_if_nonfinite"),
                "foreach": kwargs.get("foreach"),
                "parameter_count": len(params),
            }
        )
        return original_clip(params, *args, **kwargs)

    monkeypatch.setattr(torch.nn.utils, "clip_grad_norm_", wrapped_clip)

    trainer._clip_model_gradients_(model, 0.05)

    assert calls == [
        {
            "error_if_nonfinite": False,
            "foreach": True,
            "parameter_count": 2,
        }
    ]
    total_norm = torch.linalg.vector_norm(
        torch.stack([param.grad.detach().norm(2) for param in model.parameters() if param.grad is not None]),
        ord=2,
    )
    assert float(total_norm) <= 0.0501

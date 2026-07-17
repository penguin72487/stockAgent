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


def test_foreach_finite_checks_preserve_parameter_and_gradient_semantics() -> None:
    model = torch.nn.Linear(4, 3)
    model(torch.ones(2, 4)).sum().backward()

    assert trainer._model_parameters_are_finite(model)
    assert trainer._model_gradients_are_finite(model)

    with torch.no_grad():
        model.weight[0, 0] = float("inf")
    assert not trainer._model_parameters_are_finite(model)
    with torch.no_grad():
        model.weight[0, 0] = 0.0

    assert model.bias.grad is not None
    model.bias.grad[0] = float("nan")
    assert not trainer._model_gradients_are_finite(model)


def test_cuda_event_timing_can_be_disabled_without_touching_cuda(monkeypatch) -> None:
    timing = trainer.TimingBreakdown()

    def unexpected_event(*args, **kwargs):
        raise AssertionError("disabled profiling must not create CUDA events")

    monkeypatch.setattr(torch.cuda, "Event", unexpected_event)
    with trainer._cuda_timing(
        timing,
        "model_forward_cuda_s",
        torch.device("cuda"),
        enabled=False,
    ):
        pass

    assert timing.cuda_events == []

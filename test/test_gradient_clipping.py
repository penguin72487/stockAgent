import torch

from stockagent.training import trainer


def test_fast_gradient_clip_prefers_foreach_without_nonfinite_sync(monkeypatch) -> None:
    model = torch.nn.Linear(4, 3)
    loss = model(torch.ones(2, 4)).sum()
    loss.backward()

    calls: list[dict[str, object]] = []
    original_norm = torch._foreach_norm

    def wrapped_norm(gradients, *args, **kwargs):
        grads = list(gradients)
        calls.append(
            {
                "dtype": kwargs.get("dtype"),
                "parameter_count": len(grads),
            }
        )
        return original_norm(grads, *args, **kwargs)

    monkeypatch.setattr(torch, "_foreach_norm", wrapped_norm)

    trainer._clip_model_gradients_(model, 0.05)

    assert calls == [
        {
            "dtype": torch.float64,
            "parameter_count": 2,
        }
    ]
    total_norm = torch.linalg.vector_norm(
        torch.stack([param.grad.detach().norm(2) for param in model.parameters() if param.grad is not None]),
        ord=2,
    )
    assert float(total_norm) <= 0.0501


def test_large_finite_fp32_gradients_clip_without_overflow_or_erasure() -> None:
    for magnitude in (1e25, 2e38):
        model = torch.nn.Linear(2, 1, bias=False)
        model.weight.grad = torch.tensor([[magnitude, -magnitude]])
        expected_norm = torch.linalg.vector_norm(model.weight.grad.double())
        expected = model.weight.grad.double() / (expected_norm + 1e-6)
        actual_norm = trainer._clip_model_gradients_(model, 1.0)
        torch.testing.assert_close(actual_norm, expected_norm)
        torch.testing.assert_close(model.weight.grad.double(), expected, rtol=3e-7, atol=1e-8)
        assert model.weight.grad.dtype == torch.float32
        assert torch.count_nonzero(model.weight.grad) == 2


def test_stable_clip_matches_ordinary_clip_on_normal_gradients() -> None:
    torch.manual_seed(85)
    model = torch.nn.Linear(20, 4)
    reference = torch.nn.Linear(20, 4)
    for actual, expected in zip(model.parameters(), reference.parameters(), strict=True):
        actual.grad = torch.randn_like(actual)
        expected.grad = actual.grad.clone()
    norm = trainer._clip_model_gradients_(model, 0.7)
    expected_norm = torch.nn.utils.clip_grad_norm_(reference.parameters(), 0.7, foreach=True)
    torch.testing.assert_close(norm.float(), expected_norm, rtol=3e-7, atol=1e-7)
    for actual, expected in zip(model.parameters(), reference.parameters(), strict=True):
        torch.testing.assert_close(actual.grad, expected.grad, rtol=4e-7, atol=1e-8)


def test_actual_nonfinite_gradient_evidence_is_preserved() -> None:
    for bad in (float("nan"), float("inf"), -float("inf")):
        model = torch.nn.Linear(2, 1, bias=False)
        model.weight.grad = torch.tensor([[bad, 2.0]])
        before = model.weight.grad.clone()
        norm = trainer._clip_model_gradients_(model, 1.0)
        assert not torch.isfinite(norm)
        torch.testing.assert_close(model.weight.grad, before, equal_nan=True)


def test_stable_clip_without_gradients_returns_none() -> None:
    assert trainer._clip_model_gradients_(torch.nn.Linear(2, 1), 1.0) is None


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


def test_gradient_norm_diagnostic_distinguishes_zero_from_unmeasured() -> None:
    stats = torch.zeros(3)
    trainer._accumulate_gradient_norm_diagnostic_(
        torch.tensor(0.0),
        norm_sum=stats[0],
        zero_count=stats[1],
        observation_count=stats[2],
    )
    trainer._accumulate_gradient_norm_diagnostic_(
        torch.tensor(2.0),
        norm_sum=stats[0],
        zero_count=stats[1],
        observation_count=stats[2],
    )
    timing = trainer.TimingBreakdown(
        batches=2,
        gradient_norm_before_clip_sum=float(stats[0]),
        gradient_norm_zero_batches=int(stats[1]),
        gradient_norm_observations=int(stats[2]),
    )
    measured = trainer._timing_curve_payload(train_timing=timing)
    unmeasured = trainer._timing_curve_payload(
        train_timing=trainer.TimingBreakdown(batches=2)
    )

    assert measured["train_grad_norm_measured"] == 1
    assert measured["train_grad_norm_before_clip_mean"] == 1.0
    assert measured["train_zero_grad_batches"] == 1
    assert measured["train_grad_norm_observations"] == 2
    assert unmeasured["train_grad_norm_measured"] == 0
    assert unmeasured["train_grad_norm_observations"] == 0

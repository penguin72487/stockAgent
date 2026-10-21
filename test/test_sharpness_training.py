"""Independent two-pass oracles, RNG/rollback and real annual-account DDP."""
from datetime import timedelta
import multiprocessing as mp
from pathlib import Path
from types import SimpleNamespace

import pytest
import torch
import torch.distributed as dist
from torch.amp import GradScaler
import yaml

from stockagent.config import TrainingConfig, load_config
from stockagent.training import trainer
from stockagent.training.checkpoint_contract import _training_checkpoint_contract
from stockagent.training.optimizer_config import normalized_sharpness_options, sharpness_checkpoint_fields
from stockagent.training.sharpness import SharpnessPerturbation, run_sharpness_aware_epoch
from test_crypto_trajectory_optimizer import _LOSS_OPTIONS
from test_day_trade_episode_parallel import CountingSGD, PolicyWithUnused, Scheduler
from test_day_trade_training_episodes import annual_fixture, independent_year_reference


def independent_displacement(parameter, gradient, mode, radius):
    scale = parameter.abs() + .01 if mode == "asam" else torch.ones_like(parameter)
    weighted = scale * gradient
    return radius * scale * weighted / torch.linalg.vector_norm(weighted)


def independent_xsam(displacement, gradient, alpha):
    v0 = displacement / displacement.norm()
    v1 = gradient / gradient.norm()
    cosine = (v0 @ v1).clamp(-1., 1.)
    theta = torch.acos(cosine)
    if abs(float(torch.sin(theta))) < 1e-6:
        return gradient
    return gradient.norm() * (torch.sin((1-alpha)*theta)*v0 + torch.sin(alpha*theta)*v1) / torch.sin(theta)


@pytest.mark.parametrize("mode,alpha", [("sam", 1.), ("asam", 1.), ("xsam", .5), ("xsam", 1.), ("xsam", 1.5)])
def test_two_pass_update_matches_independent_quadratic_oracle(mode, alpha):
    initial = torch.tensor([.2, -.7, .9], dtype=torch.float64)
    curvature = torch.tensor([[3., .2, .1], [.2, 1., -.1], [.1, -.1, 2.]], dtype=torch.float64)
    model = torch.nn.Linear(3, 1, bias=False, dtype=torch.float64)
    with torch.no_grad(): model.weight.copy_(initial)
    parameter = model.weight
    first = curvature @ initial
    displacement = independent_displacement(initial, first, mode, .05)
    shifted = curvature @ (initial + displacement)
    expected = independent_xsam(displacement, shifted, alpha) if mode == "xsam" else shifted
    expected = expected * min(1., .1 / (float(expected.norm()) + 1e-6))
    optimizer = torch.optim.SGD(model.parameters(), lr=.2, weight_decay=.03)
    calls, snapshots = [], []
    def epoch(*, gradient_only=False, gradient_transform=None):
        optimizer.zero_grad(set_to_none=True)
        value = .5 * (parameter.flatten() @ curvature @ parameter.flatten())
        value.backward()
        snapshots.append(parameter.detach().clone())
        if gradient_transform: gradient_transform()
        if not gradient_only:
            torch.testing.assert_close(parameter.flatten(), initial, rtol=0, atol=0)
            torch.nn.utils.clip_grad_norm_(model.parameters(), .1)
            optimizer.step()
        calls.append(gradient_only)
        return value.detach(), trainer.TimingBreakdown(batches=3, optimizer_steps=int(not gradient_only), forward_s=2.)
    base, timing = run_sharpness_aware_epoch(model, epoch, mode=mode, radius=.05, alpha=alpha,
        capture_rng_state=trainer._capture_rng_state, restore_rng_state=trainer._restore_rng_state)
    torch.testing.assert_close(parameter.flatten(), initial - .2 * (expected + .03*initial), rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(snapshots[1].flatten(), initial+displacement, rtol=1e-12, atol=1e-12)
    assert calls == [True, False] and timing.optimizer_steps == 1
    assert timing.batches == 6 and timing.forward_s == 4.
    assert timing.multiobjective_diagnostics["sharpness_total_vjp_passes"] == 2
    torch.testing.assert_close(base, .5 * (initial @ curvature @ initial))
    if mode == "asam":
        assert float((displacement / (initial.abs()+.01)).norm()) == pytest.approx(.05)
    else:
        assert float(displacement.norm()) == pytest.approx(.05)


@pytest.mark.parametrize("mode", ["sam", "asam", "xsam"])
def test_second_pass_reuses_rng_and_consumes_one_logical_pass(mode):
    torch.manual_seed(813)
    model = torch.nn.Linear(2, 1, bias=False)
    # Capture after initialization; the two passes must use the same stochastic mask.
    original = trainer._capture_rng_state()
    draws = []
    optimizer = torch.optim.SGD(model.parameters(), lr=.01)
    def epoch(*, gradient_only=False, gradient_transform=None):
        optimizer.zero_grad(set_to_none=True)
        draws.append(torch.rand(2))
        value = model(draws[-1]).square().sum()
        value.backward()
        if gradient_transform: gradient_transform()
        if not gradient_only: optimizer.step()
        return value.detach(), trainer.TimingBreakdown()
    run_sharpness_aware_epoch(model, epoch, mode=mode, radius=.01, alpha=1.,
        capture_rng_state=trainer._capture_rng_state, restore_rng_state=trainer._restore_rng_state)
    actual_next = torch.rand(3)
    trainer._restore_rng_state(original, rank_local=True)
    torch.rand(2)
    torch.testing.assert_close(actual_next, torch.rand(3), rtol=0, atol=0)
    torch.testing.assert_close(draws[0], draws[1], rtol=0, atol=0)


@pytest.mark.parametrize("fail", [True, False])
def test_all_original_parameters_restore_even_when_unused_in_second_pass(fail):
    p, unused = torch.nn.Parameter(torch.tensor([.2, -.1])), torch.nn.Parameter(torch.tensor(.7))
    state = SharpnessPerturbation([p, unused], mode="sam", radius=.05)
    p.grad, unused.grad = torch.ones_like(p), torch.ones_like(unused)
    state.perturb()
    unused.grad = None
    if not fail: state.before_update()
    state.restore()
    torch.testing.assert_close(torch.cat([p.flatten(), unused.flatten()]), state.original, rtol=0, atol=0)
    assert unused.grad is None


def test_failed_second_pass_restores_before_exception_escapes():
    model = torch.nn.Linear(2, 1, bias=False)
    original = model.weight.detach().clone()
    def epoch(*, gradient_only=False, gradient_transform=None):
        if not gradient_only: raise RuntimeError("replay rejected")
        model.weight.grad = torch.ones_like(model.weight)
        return torch.tensor(1.), trainer.TimingBreakdown()
    with pytest.raises(RuntimeError, match="replay rejected"):
        run_sharpness_aware_epoch(model, epoch, mode="sam", radius=.05, alpha=1.,
            capture_rng_state=trainer._capture_rng_state, restore_rng_state=trainer._restore_rng_state)
    torch.testing.assert_close(model.weight, original, rtol=0, atol=0)


@pytest.mark.parametrize("kind", ["zero", "parallel", "antipodal", "nonfinite"])
def test_degenerate_spherical_directions_are_finite_or_fail_closed(kind):
    p = torch.nn.Parameter(torch.tensor([1., 2.], dtype=torch.float64))
    helper = SharpnessPerturbation([p], mode="xsam", radius=.05, alpha=1.5)
    p.grad = torch.zeros_like(p) if kind == "zero" else torch.tensor([1., 0.], dtype=p.dtype)
    helper.perturb()
    p.grad = -p.grad if kind == "antipodal" else p.grad.clone()
    if kind == "nonfinite":
        p.grad[0] = float("nan")
        with pytest.raises(FloatingPointError): helper.before_update()
        helper.restore()
    else:
        expected = p.grad.clone()
        helper.before_update()
        torch.testing.assert_close(p.grad, expected, rtol=0, atol=0)
        assert helper.diagnostics["sharpness_xsam_degenerate"] == 1
    torch.testing.assert_close(p, torch.tensor([1., 2.], dtype=p.dtype), rtol=0, atol=0)


@pytest.mark.parametrize("options", [("other", .1, 1), ("sam", 0, 1), ("none", .1, 1),
    ("sam", .1, 1.5), ("xsam", float("inf"), 1), ("xsam", .1, 3), ("xsam", True, 1)])
def test_invalid_sharpness_options_fail_closed(options):
    with pytest.raises(ValueError): normalized_sharpness_options(*options)


def test_disabled_contract_is_unchanged_and_each_active_mode_is_distinct(tmp_path):
    base = Path("configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml").resolve()
    original = _training_checkpoint_contract(load_config(base))
    assert sharpness_checkpoint_fields(TrainingConfig(non_blocking_transfer=False)) == {}
    raw = dict(base_config=str(base), training=dict(multi_gpu_strategy="distributed_data_parallel",
        day_trade_annual_episode_sharding=True, ddp_trajectory_gradient_sync="trajectory", ddp_broadcast_buffers=False))
    path = tmp_path/"config.yaml"
    path.write_text(yaml.safe_dump(raw))
    disabled = _training_checkpoint_contract(load_config(path))
    assert "sharpness" not in original["optimizer"] and "sharpness" not in disabled["optimizer"]
    contracts = []
    for mode in ("sam", "asam", "xsam"):
        raw["training"].update(sharpness_mode=mode, sharpness_radius=.05,
                               sharpness_alpha=1.5 if mode == "xsam" else 1.)
        path.write_text(yaml.safe_dump(raw))
        contract = _training_checkpoint_contract(load_config(path))
        assert contract != disabled
        assert contract["optimizer"]["sharpness"]["abi"] == "complete_annual_two_pass_sharpness_v1"
        contracts.append(contract)
    assert contracts[0] != contracts[1] != contracts[2]
    raw["environment"] = {"amp_dtype":"fp16"}
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="sharpness requires"): load_config(path)


def _annual_worker(rank, rendezvous, results, backend):
    torch.set_num_threads(1)
    device = torch.device("cuda", rank) if backend == "nccl" else torch.device("cpu")
    if device.type == "cuda": torch.cuda.set_device(device)
    dist.init_process_group(backend, init_method=f"file://{rendezvous}", rank=rank,
        world_size=2, timeout=timedelta(seconds=90))
    try:
        for mode in ("sam", "asam", "xsam"):
            split, runtime, loss_fn = annual_fixture()
            split = trainer._pad_windowed_training_split(split, 6)
            policy = PolicyWithUnused().to(device)
            config = SimpleNamespace(training=TrainingConfig(non_blocking_transfer=False,
                ddp_broadcast_buffers=False, ddp_trajectory_gradient_sync="trajectory",
                day_trade_annual_episode_sharding=True))
            model = trainer._wrap_distributed_data_parallel_model(policy, config=config, device=device)
            optimizer, scheduler = CountingSGD(model.parameters()), Scheduler()
            options = dict(_LOSS_OPTIONS, batch_size=6, device=device, amp_dtype=None,
                non_blocking=False, grad_clip_norm=.005, execution_runtime=runtime,
                max_volume_participation=.5, volume_participation_equity=10_000_000.,
                optimizer_step_per_trajectory=True, ddp_trajectory_gradient_sync="trajectory",
                day_trade_annual_episode_sharding=True, lr_scheduler=scheduler, lr_scheduler_interval="step")
            def epoch(**kwargs):
                output = trainer._train_epoch_windowed_tensor_ddp(model, loss_fn, split, optimizer,
                    GradScaler(device.type, enabled=False), **options, **kwargs)
                if kwargs.get("gradient_only"):
                    assert optimizer.calls == scheduler.calls == 0 and output[1].optimizer_steps == 0
                return output
            _, timing = run_sharpness_aware_epoch(model, epoch, mode=mode, radius=.01,
                alpha=1.5 if mode == "xsam" else 1., capture_rng_state=trainer._capture_rng_state,
                restore_rng_state=trainer._restore_rng_state)
            assert optimizer.calls == scheduler.calls == timing.optimizer_steps == 1
            assert policy.unused.grad is None
            results.put(dict(rank=rank, mode=mode, gradient=policy.action.grad.cpu().tolist(),
                             parameter=policy.action.detach().cpu().tolist()))
    except BaseException as error:
        results.put(dict(rank=rank, error=repr(error)))
        raise
    finally: dist.destroy_process_group()


@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("backend", ["gloo", pytest.param("nccl", marks=pytest.mark.skipif(
    torch.cuda.device_count() < 2, reason="requires two CUDA devices"))])
def test_two_rank_sharpness_matches_independent_complete_annual_accounts(tmp_path, monkeypatch, backend, deferred):
    if backend == "gloo": monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_DEFER_VALIDATION", "1" if deferred else "0")
    expected = {}
    for mode in ("sam", "asam", "xsam"):
        split, runtime, loss_fn = annual_fixture()
        policy = PolicyWithUnused()
        initial = policy.action.detach().clone()
        base = independent_year_reference(split, runtime, loss_fn, policy)
        g0, = torch.autograd.grad(base, policy.action)
        displacement = independent_displacement(initial.flatten(), g0.flatten(), mode, .01).view_as(initial)
        with torch.no_grad(): policy.action.copy_(initial + displacement)
        shifted = independent_year_reference(split, runtime, loss_fn, policy)
        g1, = torch.autograd.grad(shifted, policy.action)
        if mode == "xsam": g1 = independent_xsam(displacement.flatten(), g1.flatten(), 1.5).view_as(g1)
        with torch.no_grad(): policy.action.copy_(initial)
        policy.action.grad = g1
        torch.nn.utils.clip_grad_norm_(policy.parameters(), .005)
        optimizer = CountingSGD(policy.parameters())
        optimizer.step()
        expected[mode] = (policy.action.grad.clone(), policy.action.detach().clone())
    context = mp.get_context("spawn")
    results = context.Queue()
    workers = [context.Process(target=_annual_worker, args=(r, str(tmp_path/"init"), results, backend)) for r in range(2)]
    for worker in workers: worker.start()
    try:
        observed = [results.get(timeout=150) for _ in range(6)]
    finally:
        for worker in workers:
            worker.join(timeout=15)
            if worker.is_alive(): worker.terminate()
    assert all(worker.exitcode == 0 for worker in workers)
    for result in observed:
        assert "error" not in result, result
        grad, param = expected[result["mode"]]
        torch.testing.assert_close(torch.tensor(result["gradient"]), grad, rtol=2e-4, atol=1e-7)
        torch.testing.assert_close(torch.tensor(result["parameter"]), param, rtol=2e-6, atol=1e-7)

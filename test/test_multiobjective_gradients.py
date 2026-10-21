"""Independent gradient surgery, annual-account VJP and two-rank update oracles."""
from datetime import timedelta
import multiprocessing as mp
from pathlib import Path
import random
from types import SimpleNamespace

import numpy as np
import pytest
import torch
import torch.distributed as dist
from torch.amp import GradScaler
import yaml

from stockagent.config import TrainingConfig, load_config
from stockagent.training import trainer
from stockagent.training.checkpoint_contract import _training_checkpoint_contract
from stockagent.training.multiobjective import (
    MULTIOBJECTIVE_ABI, MGDA_L2_ABI, FRONTIER_L2_ABI, FRONTIER_L2_METHODS,
    MULTIOBJECTIVE_OBJECTIVES, TASK_NAMES, TASK_OBJECTIVES, ParameterGradientBank,
    combine_task_gradients, mgda_coefficients,
)
from stockagent.training.return_adjoint import (
    ReturnAdjointTape, account_offsets, task_return_gradients, trajectory_metric_loss,
)
from test_crypto_trajectory_optimizer import _LOSS_OPTIONS
from test_day_trade_episode_parallel import CountingSGD, PolicyWithUnused, Scheduler
from test_day_trade_training_episodes import annual_fixture, independent_year_reference


BASE = "configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml"
METHODS = ("pcgrad", "mgda", "mgda_l2", *FRONTIER_L2_METHODS)


def direct_pcgrad(rows, seed=42, step=0):
    rng = random.Random((seed << 32) + step)
    projected = rows.copy()
    for i in range(4):
        others = [j for j in range(4) if i != j]
        rng.shuffle(others)
        for j in others:
            dot = projected[i] @ rows[j]
            norm2 = rows[j] @ rows[j]
            if dot < 0 and norm2 > 0:
                projected[i] -= dot / norm2 * rows[j]
    return projected.sum(0)


@pytest.mark.parametrize("step", [0, 1, 99])
def test_gram_pcgrad_matches_independent_vector_algorithm_and_preserves_rng(step):
    rows = np.random.default_rng(71).normal(size=(4, 37))
    rows[3] = 0.
    python_state, torch_state = random.getstate(), torch.get_rng_state().clone()
    mixed, info = combine_task_gradients(torch.tensor(rows), "pcgrad", step=step)
    np.testing.assert_allclose(mixed.numpy(), direct_pcgrad(rows, step=step), rtol=1e-12, atol=1e-12)
    assert info["zero_gradient_tasks"] == 1
    assert python_state == random.getstate()
    assert torch.equal(torch_state, torch.get_rng_state())


@pytest.mark.parametrize("kind", ["random", "duplicate", "opposite", "one_zero", "all_zero", "unequal_units"])
def test_mgda_has_simplex_and_global_common_descent_certificate(kind):
    rows = np.random.default_rng(8).normal(size=(4, 17))
    if kind == "duplicate": rows[:] = rows[0]
    if kind == "opposite": rows[1] = -rows[0]
    if kind == "one_zero": rows[3] = 0.
    if kind == "all_zero": rows[:] = 0.
    if kind == "unequal_units": rows *= np.array([1e4, 1., 1e2, 1e-3])[:, None]
    gram = rows @ rows.T
    alpha = mgda_coefficients(gram)
    assert alpha.min() >= 0 and alpha.sum() == pytest.approx(1.)
    direction = alpha @ rows
    products = rows @ direction
    norm2 = direction @ direction
    assert products.min() >= norm2 - 1e-8 * max(1., abs(products).max())
    # The certificate implies global optimality over EVERY convex combination.
    weights = np.random.default_rng(34).dirichlet(np.ones(4), size=1000)
    assert np.square(weights @ rows).sum(1).min() >= norm2 - 1e-9
    if kind in {"opposite", "one_zero", "all_zero"}:
        np.testing.assert_allclose(direction, 0., atol=1e-10)


def test_return_space_mgda_is_not_a_parameter_space_common_descent_guarantee():
    rows = torch.tensor([[1., 0.], [-1., 1.], [1., 0.], [-1., 1.]], dtype=torch.float64)
    mixed, _ = combine_task_gradients(rows, "mgda")
    assert bool((rows @ mixed > 0).all())
    jacobian = torch.tensor([[1., 0.], [-2., 1.]], dtype=torch.float64)
    parameter_rows = rows @ jacobian
    parameter_direction = mixed @ jacobian
    assert (parameter_rows @ parameter_direction)[0] < 0
    correct, _ = combine_task_gradients(parameter_rows, "mgda")
    assert bool((parameter_rows @ correct >= correct.square().sum() - 1e-12).all())


def test_unit_mgda_is_invariant_to_objective_units_and_matches_geometric_oracle():
    rows = torch.tensor([[2., 0.], [0., 3.], [5., 0.], [0., 7.]], dtype=torch.float64)
    expected = torch.tensor([.5, .5], dtype=torch.float64)
    mixed, info = combine_task_gradients(rows, "mgda_l2")
    torch.testing.assert_close(mixed, expected, rtol=0, atol=1e-12)
    scaled, _ = combine_task_gradients(rows * torch.tensor(
        [1e-9, 1e8, 1e3, 1e-4], dtype=torch.float64)[:, None], "mgda_l2")
    torch.testing.assert_close(scaled, mixed, rtol=0, atol=1e-12)
    assert sum(info[f"coefficient_{name}"] for name in TASK_NAMES) == pytest.approx(1.)
    assert all(info[f"raw_descent_derivative_{name}"] < 0 for name in TASK_NAMES)
    assert info["mgda_kkt_margin"] >= -1e-12


@pytest.mark.parametrize("kind", ["random", "one_zero", "all_zero", "opposite"])
def test_unit_mgda_retains_zero_tasks_and_sgd_preserves_common_descent(kind):
    rows = torch.tensor(np.random.default_rng(8).normal(size=(4, 17)), dtype=torch.float64)
    if kind == "one_zero": rows[3] = 0.
    if kind == "all_zero": rows[:] = 0.
    if kind == "opposite": rows[1] = -rows[0]
    mixed, info = combine_task_gradients(rows, "mgda_l2")
    p = torch.nn.Parameter(torch.zeros(17, dtype=torch.float64))
    p.grad = mixed.clone()
    torch.nn.utils.clip_grad_norm_([p], .01)
    torch.optim.SGD([p], lr=.03, momentum=0, weight_decay=0).step()
    assert bool((rows @ p.detach() <= 1e-12).all())
    assert info["mgda_kkt_margin"] >= -1e-12
    if kind != "random":
        torch.testing.assert_close(mixed, torch.zeros_like(mixed), rtol=0, atol=1e-12)


@pytest.mark.parametrize("method", FRONTIER_L2_METHODS)
@pytest.mark.parametrize("kind", ["random", "duplicate", "opposite", "nearly_opposite", "one_zero", "all_zero"])
def test_frontier_gram_matches_independent_full_vector_oracles(method, kind):
    rows = np.random.default_rng(231).normal(size=(4, 19))
    if kind == "duplicate": rows[:] = rows[0]
    if kind == "opposite": rows[1] = -rows[0]
    if kind == "nearly_opposite": rows[1] = -rows[0] + 1e-9 * rows[2]
    if kind == "one_zero": rows[2] = 0.
    if kind == "all_zero": rows[:] = 0.
    tensor = torch.tensor(rows, dtype=torch.float64)
    reference, _ = combine_task_gradients(tensor, "mgda_l2")
    units = rows / np.where(np.linalg.norm(rows, axis=1) > 0,
                           np.linalg.norm(rows, axis=1), 1.)[:, None]
    if reference.norm() <= (64 * np.finfo(np.float64).eps) ** .5:
        expected = reference.numpy()
    elif method == "config_l2":
        # Q's rcond=1e-12 corresponds to U's singular-value rcond=1e-6.
        expected = np.linalg.pinv(units, rcond=1e-6) @ np.ones(4)
        if np.min(units @ expected) < -1e-10:
            expected = reference.numpy()
        expected *= float(reference.norm()) / np.linalg.norm(expected)
    elif method == "upgrad_log_l2":
        from scipy.optimize import minimize
        anchor = units[1]
        result = minimize(lambda x: .5 * np.square(x - anchor).sum(), np.zeros_like(anchor),
                          jac=lambda x: x - anchor,
                          constraints={"type": "ineq", "fun": lambda x: units @ x,
                                       "jac": lambda x: units},
                          method="SLSQP", options={"ftol": 1e-13, "maxiter": 100})
        assert result.success
        expected = result.x * float(reference.norm()) / np.linalg.norm(result.x)
    else:
        displacement = np.zeros(rows.shape[1])
        for _ in range(int(method.rsplit("_", 1)[1])):
            distances = np.linalg.norm(displacement[None, :] + units, axis=1)
            displacement -= .2 * np.sum(distances[:, None] * units, axis=0)
        expected = -displacement
        expected *= float(reference.norm()) / np.linalg.norm(expected)
    mixed, info = combine_task_gradients(tensor, method)
    np.testing.assert_allclose(mixed.numpy(), expected, rtol=1e-10, atol=1e-12)
    assert float(mixed.norm()) == pytest.approx(float(reference.norm()), abs=1e-12)
    assert "mgda_kkt_margin" not in info  # No false KKT claim for the new direction.
    assert info["reference_mgda_kkt_margin"] >= -1e-9
    if method in {"config_l2", "upgrad_log_l2"}:
        assert float((tensor @ mixed).min()) >= -1e-9
    scaled, _ = combine_task_gradients(tensor * torch.tensor(
        [1e-7, 1e6, .03, 2e3], dtype=torch.float64)[:, None], method)
    if kind == "nearly_opposite":
        # Inside Gram roundoff, preserve the parent MGDA vector for EACH
        # input; do not amplify it. The near-singular parent simplex solve
        # itself need not be bitwise invariant to normalization roundoff.
        scaled_reference, _ = combine_task_gradients(tensor * torch.tensor(
            [1e-7, 1e6, .03, 2e3], dtype=torch.float64)[:, None], "mgda_l2")
        torch.testing.assert_close(scaled, scaled_reference, rtol=0, atol=0)
    else:
        torch.testing.assert_close(scaled, mixed, rtol=1e-9, atol=1e-12)
    # The log-preferred method keeps its named log-return row at index 1.
    order = [2, 1, 3, 0] if method == "upgrad_log_l2" else [2, 0, 3, 1]
    permuted, _ = combine_task_gradients(tensor[order], method)
    if kind == "nearly_opposite":
        permuted_reference, _ = combine_task_gradients(tensor[order], "mgda_l2")
        torch.testing.assert_close(permuted, permuted_reference, rtol=0, atol=0)
    else:
        torch.testing.assert_close(permuted, mixed, rtol=1e-9, atol=1e-12)


def test_config_singular_conflicting_least_squares_uses_certified_mgda_fallback():
    # A common descent direction exists, but the overdetermined equal-alignment
    # least-squares solution opposes one row. Rank deficiency must not hide it.
    angles = np.array([-1.5091920353466102, 1.467925984692512,
                       -0.4720620974087253, -1.3901039172225234])
    rows = torch.tensor(np.stack([np.cos(angles), np.sin(angles)], axis=1))
    mixed, info = combine_task_gradients(rows, "config_l2")
    assert info["direction_fallback_mgda"] == 1
    assert (rows @ mixed).min() >= -1e-12
    reference, _ = combine_task_gradients(rows, "mgda_l2")
    torch.testing.assert_close(mixed, reference, rtol=0, atol=1e-12)


def test_four_objectives_use_log_return_and_preserve_complete_account_resets():
    r = torch.tensor([-.04, .03, -.02, .01, -.06], dtype=torch.float64, requires_grad=True)
    values, gradients = task_return_gradients(r, [0, 2, 5])
    for i, objective in enumerate(TASK_OBJECTIVES):
        loss = trajectory_metric_loss(r, objective, [0, 2, 5])
        grad, = torch.autograd.grad(loss, r)
        torch.testing.assert_close(values[i], loss)
        torch.testing.assert_close(gradients[i], grad)
    torch.testing.assert_close(values[1], -252 * r.mean())
    assert TASK_NAMES[-1] == "calmar" and TASK_OBJECTIVES[-1] == "trajectory_calmar"
    mdd_gradient, = torch.autograd.grad(trajectory_metric_loss(r, "trajectory_mdd", [0, 2, 5]), r)
    assert not torch.allclose(gradients[-1], mdd_gradient)
    assert values[1].item() != pytest.approx(trajectory_metric_loss(r, "trajectory_return", [0, 2, 5]).item())
    for objective in MULTIOBJECTIVE_OBJECTIVES:
        torch.testing.assert_close(trajectory_metric_loss(r, objective), -252 * r.mean())


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("device_name", ["cpu", pytest.param("cuda", marks=pytest.mark.skipif(
    not torch.cuda.is_available(), reason="requires CUDA"))])
def test_parameter_bank_matches_direct_update_and_leaves_unused_parameters_absent(method, device_name):
    device = torch.device(device_name, torch.cuda.current_device()) if device_name == "cuda" else torch.device("cpu")
    parameters = [torch.nn.Parameter(torch.tensor([.3, -.1], device=device)),
                  torch.nn.Parameter(torch.tensor(.7, device=device))]
    bank = ParameterGradientBank(parameters)
    rows = torch.tensor([[1., 0.], [-1., 1.], [.1, -.4], [.5, .2]], device=device)
    for row in rows[:3]:
        parameters[0].grad = row.clone()
        bank.record()
    parameters[0].grad = rows[3].clone()
    expected, _ = combine_task_gradients(rows, method)
    before = parameters[0].detach().clone()
    info = bank.combine(method, seed=42, step=0)
    assert parameters[1].grad is None
    torch.testing.assert_close(parameters[0].grad, expected)
    optimizer = torch.optim.AdamW(parameters, lr=.001, weight_decay=.1)
    optimizer.step()
    delta = parameters[0].detach() - before
    observed = bank.observe_update()
    for i, name in enumerate(TASK_NAMES):
        assert observed[f"actual_update_derivative_{name}"] == pytest.approx(float(rows[i].double() @ delta.double()), abs=1e-10)
    assert float(parameters[1].detach()) == pytest.approx(.7)
    assert info["combined_norm"] >= 0


@pytest.mark.parametrize("damage", ["nan", "shape", "method"])
def test_surgery_fails_closed(damage):
    rows = torch.ones((4, 3), dtype=torch.float64)
    if damage == "nan": rows[0, 1] = float("nan")
    if damage == "shape": rows = rows[:3]
    with pytest.raises((ValueError, FloatingPointError)):
        combine_task_gradients(rows, "unknown" if damage == "method" else "mgda")


@pytest.mark.parametrize("objective", sorted(MULTIOBJECTIVE_OBJECTIVES))
def test_configs_fingerprint_each_surgery_space_and_reject_auxiliary_losses(tmp_path, objective):
    raw = {"base_config": str(Path(BASE).resolve()), "training": dict(
        loss_type=objective, multi_gpu_strategy="distributed_data_parallel",
        day_trade_annual_episode_sharding=True, ddp_broadcast_buffers=False,
        ddp_trajectory_gradient_sync="trajectory")}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    config = load_config(path)
    contract = _training_checkpoint_contract(config)
    assert contract != _training_checkpoint_contract(load_config(BASE))
    surgery = contract["fold_continuation"]["return_adjoint"]
    assert surgery["objective"] == objective
    assert surgery["validation_selection"] == "pure_log_utility_no_test_selection"
    assert surgery["tasks"] == list(TASK_OBJECTIVES)
    is_frontier = any(f"trajectory_{method}_" in objective for method in FRONTIER_L2_METHODS)
    assert surgery["abi"] == (FRONTIER_L2_ABI if is_frontier else
                              MGDA_L2_ABI if "mgda_l2" in objective else MULTIOBJECTIVE_ABI)
    assert surgery["abi"] != "annual_four_metric_raw_gram_surgery_v1"
    assert (surgery["scaling"] == "unit_global_l2_task_gradients_zero_tasks_retained") == ("mgda_l2" in objective or is_frontier)
    assert surgery["calmar_task"]["abi"] == "annual_calmar_return_adjoint_v1"
    assert surgery["calmar_task"]["calmar_drawdown_epsilon"] == 1e-8
    raw["evaluation"] = {"gamma_turnover": .01}
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="without auxiliary losses"):
        load_config(path)


def test_inherited_ablation_preserves_the_four_independent_losses_and_convergence_settings():
    from scripts.run_ablation_experiments import _experiment_rows
    spec, rows = _experiment_rows(Path("configs/ablations/tw_multiobjective_convergence_last_fold_20261016_v1.yaml"))
    assert len(rows) == 9 and rows[0]["name"] == "baseline"
    assert {row["name"] for row in rows} >= {"loss__sharpe", "loss__return", "loss__sortino", "loss__mdd"}
    fixed = spec["matrix"]["fixed_overrides"]["training"]
    assert fixed["epochs"] == 1000 and fixed["early_stopping_no_improve_ratio"] == .1
    assert fixed["lookback"] == fixed["financial_transformer"]["d_model"] == 32


def test_scratch_calmar_surgery_inherits_measured_neural_recipe_without_old_weights():
    from scripts.run_ablation_experiments import _experiment_rows
    spec, rows = _experiment_rows(Path("configs/ablations/tw_scratch_multiobjective_20261018_v1.yaml"))
    assert [row["name"] for row in rows] == [
        "gradient__pcgrad_return", "gradient__mgda_return",
        "gradient__pcgrad_parameter", "gradient__mgda_parameter",
    ]
    assert {row["overrides"]["training"]["loss_type"] for row in rows} == (
        MULTIOBJECTIVE_OBJECTIVES - {"trajectory_mgda_l2_parameter"} - {
            f"trajectory_{method}_parameter" for method in FRONTIER_L2_METHODS})
    assert spec["fold_selection"] == "last"
    fixed = spec["matrix"]["fixed_overrides"]["training"]
    assert fixed["epochs"] == 1000 and fixed["early_stopping_no_improve_ratio"] == .1
    assert fixed["lookback"] == fixed["financial_transformer"]["d_model"] == 32
    assert fixed["batch_size_train"] == 128 and fixed["batch_size_eval"] == 16
    assert fixed["pretrained_initialization_root"] is None
    assert fixed["pretrained_initialization_trainable_parameter_prefixes"] == []
    assert fixed["financial_transformer"]["compiled_cross_attention_backend"] == "manual_fp32_last"
    assert spec["matrix"]["base_overrides"]["training"]["day_trade_sub_lot_recovery"] is False


def _physical_worker(rank, rendezvous, results, backend):
    torch.set_num_threads(1)
    device = torch.device("cuda", rank) if backend == "nccl" else torch.device("cpu")
    if device.type == "cuda": torch.cuda.set_device(device)
    dist.init_process_group(backend, init_method=f"file://{rendezvous}", rank=rank,
                            world_size=2, timeout=timedelta(seconds=90))
    try:
        for method in METHODS:
            for space in ("return", "parameter"):
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
                    return trainer._train_epoch_windowed_tensor_ddp(model, loss_fn, split, optimizer,
                        GradScaler(device.type, enabled=False), **options, **kwargs)
                original = policy.action.detach().clone()
                tape = ReturnAdjointTape.create(5, device)
                epoch(return_tape=tape, capture_only=True)
                assert optimizer.calls == scheduler.calls == 0
                offsets = account_offsets(model.__dict__["_annual_episode_plan"][1].accounts_by_rank, 5)
                _, rows = task_return_gradients(tape.values, offsets)
                if space == "return":
                    mixed, _ = combine_task_gradients(rows, method)
                    replay = ReturnAdjointTape.create(5, device, expected=tape.values)
                    epoch(log_return_weights=mixed * (-5 / 252.), return_tape=replay)
                else:
                    bank = ParameterGradientBank(model.parameters())
                    for i, row in enumerate(rows):
                        replay = ReturnAdjointTape.create(5, device, expected=tape.values)
                        def combine(): bank.combine(method, seed=42, step=0)
                        _, timing = epoch(log_return_weights=row * (-5 / 252.), return_tape=replay,
                            gradient_only=i < 3, gradient_transform=combine if i == 3 else None)
                        assert timing.optimizer_steps == int(i == 3)
                        if i < 3:
                            bank.record()
                            assert optimizer.calls == scheduler.calls == 0
                            torch.testing.assert_close(policy.action, original, rtol=0, atol=0)
                assert optimizer.calls == scheduler.calls == 1
                assert policy.unused.grad is None
                accepted_gradient = policy.action.grad.cpu().tolist()
                before_rejection = policy.action.detach().clone()
                damaged = ReturnAdjointTape.create(5, device, expected=tape.values + .1)
                def forbidden_transform():
                    raise AssertionError("surgery ran before the physical return replay was accepted")
                with pytest.raises(RuntimeError, match="replay changed"):
                    epoch(log_return_weights=rows[0] * (-5 / 252.), return_tape=damaged,
                          gradient_transform=forbidden_transform)
                assert optimizer.calls == scheduler.calls == 1
                torch.testing.assert_close(policy.action, before_rejection, rtol=0, atol=0)
                results.put(dict(rank=rank, method=method, space=space,
                    gradient=accepted_gradient, parameter=policy.action.detach().cpu().tolist()))
    except BaseException as error:
        results.put(dict(rank=rank, error=repr(error)))
        raise
    finally:
        dist.destroy_process_group()


@pytest.mark.parametrize('deferred', [False, True])
@pytest.mark.parametrize("backend", ["gloo", pytest.param("nccl", marks=pytest.mark.skipif(
    torch.cuda.device_count() < 2, reason="requires two CUDA devices"))])
def test_two_rank_physical_surgery_matches_four_monolithic_account_gradients(tmp_path, monkeypatch, backend, deferred):
    import stockagent.training.loss as loss_module
    if backend == "gloo": monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_DEFER_VALIDATION', '1' if deferred else '0')
    captured = []
    original_run = loss_module.run_backtest_torch
    def capture(*args, **kwargs):
        result = original_run(*args, **kwargs)
        captured.append(result.strategy_returns)
        return result
    monkeypatch.setattr(loss_module, "run_backtest_torch", capture)
    references = {}
    for method in METHODS:
        for space in ("return", "parameter"):
            captured.clear()
            split, runtime, loss_fn = annual_fixture()
            policy = PolicyWithUnused()
            independent_year_reference(split, runtime, loss_fn, policy)
            returns = torch.cat(captured)
            losses = [trajectory_metric_loss(returns, objective, [0, 2, 5]) for objective in TASK_OBJECTIVES]
            if space == "parameter":
                task_rows = torch.stack([torch.autograd.grad(loss, policy.action, retain_graph=True)[0].flatten() for loss in losses])
                mixed, _ = combine_task_gradients(task_rows, method)
                gradient = mixed.view_as(policy.action)
            else:
                _, task_rows = task_return_gradients(returns, [0, 2, 5])
                mixed, _ = combine_task_gradients(task_rows, method)
                gradient, = torch.autograd.grad(returns, policy.action, grad_outputs=mixed)
            policy.action.grad = gradient
            torch.nn.utils.clip_grad_norm_(policy.parameters(), .005)
            optimizer = CountingSGD(policy.parameters())
            optimizer.step()
            references[method, space] = (policy.action.grad.clone(), policy.action.detach().clone())
    context = mp.get_context("spawn")
    results = context.Queue()
    workers = [context.Process(target=_physical_worker, args=(rank, str(tmp_path / "init"), results, backend)) for rank in range(2)]
    for worker in workers: worker.start()
    observed = [results.get(timeout=150) for _ in range(2 * len(METHODS) * 2)]
    for worker in workers:
        worker.join(timeout=15)
        if worker.is_alive(): worker.terminate()
        assert worker.exitcode == 0
    for result in observed:
        assert "error" not in result, result
        gradient, parameter = references[result["method"], result["space"]]
        torch.testing.assert_close(torch.tensor(result["gradient"]), gradient, rtol=2e-4, atol=1e-7)
        torch.testing.assert_close(torch.tensor(result["parameter"]), parameter, rtol=2e-6, atol=1e-7)

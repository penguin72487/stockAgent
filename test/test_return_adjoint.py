"""Whole-account metric oracles and physical, uneven two-rank return VJPs."""
from dataclasses import replace
from datetime import timedelta
import multiprocessing as mp
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import torch
import torch.distributed as dist
from torch.amp import GradScaler
import yaml

from stockagent.config import TrainingConfig, load_config
from stockagent.training import trainer
from stockagent.training.checkpoint_contract import _normalize_risk_objective, _training_checkpoint_contract
from stockagent.training.return_adjoint import (
    CALMAR_DRAWDOWN_EPS, RATIO_VARIANCE_EPS, TRAJECTORY_OBJECTIVES, ReturnAdjointTape,
    account_offsets, return_adjoint_contract, return_adjoint_weights, trajectory_metric_loss,
)
from test_crypto_trajectory_optimizer import _LOSS_OPTIONS
from test_day_trade_episode_parallel import CountingSGD, PolicyWithUnused, Scheduler
from test_day_trade_training_episodes import annual_fixture, independent_year_reference


OBJECTIVES = sorted(TRAJECTORY_OBJECTIVES)
BASE = "configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml"


def numpy_loss(r, objective):
    if objective == "trajectory_return":
        return -np.expm1(r.sum())
    if objective == "trajectory_sharpe":
        return -np.sqrt(252) * r.mean() / np.sqrt(r.var() + RATIO_VARIANCE_EPS)
    if objective == "trajectory_sortino":
        return -np.sqrt(252) * r.mean() / np.sqrt(np.minimum(r, 0).dot(np.minimum(r, 0)) / len(r) + RATIO_VARIANCE_EPS)
    nav = np.r_[1., np.exp(np.cumsum(r))]
    drawdown = np.max(1 - nav / np.maximum.accumulate(nav))
    if objective == "trajectory_calmar":
        return -np.expm1(252 * r.mean()) / max(drawdown, CALMAR_DRAWDOWN_EPS)
    return drawdown


@pytest.mark.parametrize("objective", OBJECTIVES)
def test_single_metric_matches_independent_numpy_and_complete_account_resets(objective):
    r = np.array([-.05, .09, -.03, .02, -.06, .01, .02])
    expected = (3 * numpy_loss(r[:3], objective) + 4 * numpy_loss(r[3:], objective)) / 7
    value = trajectory_metric_loss(torch.tensor(r), objective, [0, 3, 7])
    assert value.item() == pytest.approx(expected, rel=1e-12, abs=1e-12)
    if objective in {"trajectory_return", "trajectory_mdd", "trajectory_calmar"}:
        assert value.item() != pytest.approx(numpy_loss(r, objective), abs=1e-5)


@pytest.mark.parametrize("objective", OBJECTIVES + ["log_utility"])
def test_global_return_adjoint_matches_monolithic_chain_rule_with_uneven_chunks(objective):
    theta = torch.tensor([.7, -.4], dtype=torch.float64, requires_grad=True)
    x = torch.tensor([[.05, -.02], [-.08, .03], [.02, -.01], [-.04, -.09], [.01, .06]], dtype=torch.float64)
    r = (x @ theta).tanh() * .1
    direct = trajectory_metric_loss(r, objective, [0, 2, 5])
    gradient, = torch.autograd.grad(direct, theta)
    value, weights = return_adjoint_weights(r, objective, [0, 2, 5])
    assert value.item() == pytest.approx(direct.item(), abs=1e-12)
    actual = torch.zeros_like(theta)
    # This is the trainer's date weighting and DDP mean compensation, including
    # unequal owned dates and different numbers of local blocks.
    for rank_spans in [[(0, 2)], [(2, 4), (4, 5)]]:
        local = torch.zeros_like(theta)
        for start, stop in rank_spans:
            chunk = (x[start:stop] @ theta).tanh() * .1
            proxy = -252 * (chunk * weights[start:stop]).mean()
            grad, = torch.autograd.grad(proxy * (stop - start) / 5 * 2, theta)
            local += grad
        actual += local / 2
    torch.testing.assert_close(actual, gradient, rtol=1e-12, atol=1e-12)


@pytest.mark.parametrize("objective", OBJECTIVES)
@pytest.mark.parametrize("returns", [[0., 0., 0.], [.01, .02, .01], [-.1]])
def test_cash_positive_paths_and_initial_nav_are_finite(objective, returns):
    r = torch.tensor(returns, dtype=torch.float64, requires_grad=True)
    loss = trajectory_metric_loss(r, objective)
    loss.backward()
    assert torch.isfinite(loss) and torch.isfinite(r.grad).all()
    if objective == "trajectory_mdd" and returns == [-.1]:
        assert loss.item() == pytest.approx(1 - np.exp(-.1))
    if objective == "trajectory_mdd" and min(returns) >= 0:
        assert loss.item() == 0  # Pure MDD must not secretly reward return.


def test_calmar_gradient_matches_finite_differences_with_unique_drawdown():
    returns = torch.tensor([.02, -.03, -.01, .015, .01], dtype=torch.float64,
                           requires_grad=True)
    assert torch.autograd.gradcheck(
        lambda r: trajectory_metric_loss(r, "trajectory_calmar"), (returns,),
        eps=1e-6, atol=1e-5, rtol=1e-5)


def test_calmar_cash_has_finite_profit_gradient_and_separate_resume_contract():
    returns = torch.zeros(252, dtype=torch.float64, requires_grad=True)
    loss = trajectory_metric_loss(returns, "trajectory_calmar")
    loss.backward()
    assert loss.item() == 0
    torch.testing.assert_close(returns.grad, torch.full_like(returns, -1 / CALMAR_DRAWDOWN_EPS))
    contract = return_adjoint_contract("trajectory_calmar")
    assert contract['abi'] != return_adjoint_contract("trajectory_mdd")['abi']
    assert contract['calmar_drawdown_epsilon'] == CALMAR_DRAWDOWN_EPS


def test_calmar_absorbing_near_ruin_is_finite_with_initial_capital_in_drawdown():
    returns = torch.tensor([-1000., 0., 0.], dtype=torch.float64, requires_grad=True)
    loss = trajectory_metric_loss(returns, "trajectory_calmar")
    loss.backward()
    assert loss.item() == pytest.approx(1.)
    assert torch.isfinite(returns.grad).all()


def test_tape_rejects_missing_duplicate_nan_and_replay_drift():
    for damage in ["missing", "duplicate", "nan", "drift"]:
        expected = torch.tensor([.1, -.2], dtype=torch.float64) if damage == "drift" else None
        tape = ReturnAdjointTape.create(2, torch.device("cpu"), expected=expected)
        values = torch.tensor([.1, -.3 if damage == "drift" else -.2])
        if damage == "nan":
            values[0] = float("nan")
        tape.record(0, values, torch.tensor([True, damage != "missing"]))
        if damage == "duplicate":
            tape.record(0, values, torch.ones(2, dtype=torch.bool))
        with pytest.raises(RuntimeError):
            tape.finish()
    tape = ReturnAdjointTape.create(2, torch.device("cpu"))
    tape.record(0, torch.tensor([.1, .2, float("nan")]), torch.tensor([True, True, False]))
    assert tape.finish().tolist() == pytest.approx([.1, .2])
    with pytest.raises(FloatingPointError):
        trajectory_metric_loss(torch.tensor([float("inf")]), "trajectory_mdd")
    with pytest.raises(ValueError):
        account_offsets([[(2024, 0, 2)], [(2025, 1, 3)]], 3)


def test_deferred_predicate_snapshot_cannot_be_rewritten_or_left_unobserved():
    from stockagent.backtest.tw_day_trade_graph import (
        DeferredLedgerValidation, DeferredLedgerValidationError, ledger_predicate)
    flag=torch.tensor(False)
    with pytest.raises(DeferredLedgerValidationError,match='before update'):
        with DeferredLedgerValidation(torch.device('cpu')) as gate:
            assert ledger_predicate(flag,'failed_original_value')
            flag.fill_(True)
            gate.finish()
    with pytest.raises(RuntimeError,match='never observed'):
        with DeferredLedgerValidation(torch.device('cpu')):
            ledger_predicate(flag,'must_be_checked')


def _deferred_retry_worker(rank,rendezvous,results,backend='gloo'):
    import random
    import os
    from stockagent.backtest.tw_day_trade_graph import (
        ledger_predicate, ledger_validation_is_deferred, finish_ledger_validation)
    torch.set_num_threads(1)
    os.environ['STOCKAGENT_DAY_TRADE_DEFER_VALIDATION']='1'
    device=torch.device('cuda',rank) if backend=='nccl' else torch.device('cpu')
    if device.type=='cuda':torch.cuda.set_device(device)
    dist.init_process_group(backend,init_method=f'file://{rendezvous}',rank=rank,
                            world_size=2,timeout=timedelta(seconds=30))
    try:
        model=torch.nn.Linear(1,1,bias=False).to(device)
        with torch.no_grad():model.weight.fill_(.4)
        optimizer=CountingSGD(model.parameters())
        tape=ReturnAdjointTape.create(1,device)
        draws=[]

        @trainer._defer_annual_ledger_validation
        def pass_once(*,model,optimizer,scaler,device,execution_runtime,
                      day_trade_annual_episode_sharding,optimizer_step_per_trajectory,return_tape):
            draws.append((torch.rand(()).item(),torch.rand((),device=device).item(),
                          float(np.random.random()),random.random()))
            amount=sum(draws[-1])
            return_tape.record(0,torch.tensor([amount],device=device),torch.tensor([True],device=device))
            loss=model.weight.square().sum()*amount
            loss.backward()
            speculative=ledger_validation_is_deferred()
            # Only rank0 reports a failed speculative proof. Both ranks must
            # discard their gradients and RNG advances before either updates.
            assert ledger_predicate(torch.tensor(not speculative or rank==1,device=device),'rank0_fault')
            assert optimizer.calls==0
            finish_ledger_validation()
            optimizer.step()
            return loss.detach(),trainer.TimingBreakdown()

        torch.manual_seed(411+rank);np.random.seed(411+rank);random.seed(411+rank)
        with pytest.warns(RuntimeWarning,match='discarded the entire pass'):
            _,timing=pass_once(model=model,optimizer=optimizer,scaler=GradScaler(device.type,enabled=False),
                device=device,execution_runtime=SimpleNamespace(day_trade_carry_source=object()),
                day_trade_annual_episode_sharding=True,optimizer_step_per_trajectory=True,return_tape=tape)
        assert draws[0]==draws[1] and len(draws)==2
        assert optimizer.calls==1 and timing.deferred_validation_retries==1
        assert tape.counts.tolist()==[1]
        expected=.4-.001*(.8*sum(draws[0])+.004)
        assert model.weight.item()==pytest.approx(expected,abs=1e-7)
        results.put(dict(rank=rank,accepted=True,retries=timing.deferred_validation_retries))
    except BaseException as error:
        results.put(dict(rank=rank,error=repr(error)))
        raise
    finally:
        dist.destroy_process_group()


@pytest.mark.parametrize('backend',['gloo',pytest.param('nccl',marks=pytest.mark.skipif(
    torch.cuda.device_count()<2,reason='requires two CUDA devices'))])
def test_one_rank_failed_proof_retries_every_rank_without_a_stale_gradient_or_rng(tmp_path,monkeypatch,backend):
    if backend=='gloo':monkeypatch.setenv('CUDA_VISIBLE_DEVICES','')
    context=mp.get_context('spawn');results=context.Queue()
    workers=[context.Process(target=_deferred_retry_worker,args=(rank,str(tmp_path/'retry-init'),results,backend))
             for rank in range(2)]
    for worker in workers:worker.start()
    observed=[results.get(timeout=60) for _ in range(2)]
    for worker in workers:
        worker.join(timeout=10)
        if worker.is_alive():worker.terminate()
        assert worker.exitcode==0
    assert all(row.get('accepted') for row in observed),observed


@pytest.mark.parametrize("objective", OBJECTIVES)
def test_new_metric_config_is_explicit_and_has_incompatible_resume(tmp_path, objective):
    config = load_config(BASE)
    original = _training_checkpoint_contract(config)
    assert "return_adjoint" not in original["fold_continuation"]
    # Preserve the selected config inheritance via an absolute parent.
    raw = {"base_config": str(Path(BASE).resolve()), "training": {
        "loss_type": objective, "multi_gpu_strategy": "distributed_data_parallel",
        "day_trade_annual_episode_sharding": True,
        "ddp_broadcast_buffers": False, "ddp_trajectory_gradient_sync": "trajectory"}}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    changed = load_config(path)
    assert _normalize_risk_objective(changed.training.loss_type) == objective
    contract = _training_checkpoint_contract(changed)
    assert contract != original and contract["objective"] == objective
    assert contract["fold_continuation"]["return_adjoint"]["objective"] == objective
    bad = replace(changed, training=replace(changed.training, loss_type="log_utility"))
    assert "return_adjoint" not in _training_checkpoint_contract(bad)["fold_continuation"]
    raw["evaluation"] = {"gamma_turnover": .1}
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="without auxiliary losses"):
        load_config(path)


def _physical_worker(rank, rendezvous, results):
    torch.set_num_threads(1)
    dist.init_process_group("gloo", init_method=f"file://{rendezvous}", rank=rank,
                            world_size=2, timeout=timedelta(seconds=60))
    try:
        for objective in OBJECTIVES:
            split, runtime, loss_fn = annual_fixture()
            split = trainer._pad_windowed_training_split(split, 6)
            policy = PolicyWithUnused()
            config = SimpleNamespace(training=TrainingConfig(non_blocking_transfer=False,
                ddp_broadcast_buffers=False, ddp_trajectory_gradient_sync="trajectory",
                day_trade_annual_episode_sharding=True))
            model = trainer._wrap_distributed_data_parallel_model(policy, config=config, device=torch.device("cpu"))
            optimizer, scheduler = CountingSGD(model.parameters()), Scheduler()
            options = dict(_LOSS_OPTIONS, batch_size=6, device=torch.device("cpu"),
                amp_dtype=None, non_blocking=False, grad_clip_norm=0., execution_runtime=runtime,
                max_volume_participation=.5, volume_participation_equity=10_000_000.,
                optimizer_step_per_trajectory=True, ddp_trajectory_gradient_sync="trajectory",
                day_trade_annual_episode_sharding=True, lr_scheduler=scheduler, lr_scheduler_interval="step")
            def epoch(**kwargs):
                return trainer._train_epoch_windowed_tensor_ddp(model, loss_fn, split, optimizer,
                    GradScaler("cpu", enabled=False), **options, **kwargs)
            original = policy.action.detach().clone()
            if objective == "trajectory_return":
                single = ReturnAdjointTape.create(5, torch.device("cpu"))
                _, timing = epoch(return_tape=single, compound_return_adjoint=True)
                loss = trajectory_metric_loss(single.values, objective, [0, 2, 5])
                assert timing.optimizer_steps == optimizer.calls == scheduler.calls == 1
                results.put((rank, objective, {"loss": loss.item(), "gradient": policy.action.grad.tolist(),
                                             "parameter": policy.action.detach().tolist()}))
                continue
            record = ReturnAdjointTape.create(5, torch.device("cpu"))
            _, timing = epoch(return_tape=record, capture_only=True)
            assert timing.optimizer_steps == optimizer.calls == scheduler.calls == 0
            assert policy.action.grad is None
            torch.testing.assert_close(policy.action, original, atol=0, rtol=0)
            offsets = account_offsets(model.__dict__["_annual_episode_plan"][1].accounts_by_rank, 5)
            loss, weights = return_adjoint_weights(record.values, objective, offsets)
            replay = ReturnAdjointTape.create(5, torch.device("cpu"), expected=record.values)
            _, timing = epoch(log_return_weights=weights, return_tape=replay)
            assert timing.optimizer_steps == optimizer.calls == scheduler.calls == 1
            assert replay.maximum_replay_error == 0
            assert policy.unused.grad is None
            results.put((rank, objective, {"loss": loss.item(), "gradient": policy.action.grad.tolist(),
                                         "parameter": policy.action.detach().tolist()}))
            # The mismatch gate must run before any real optimizer/scheduler.
            optimizer.calls = scheduler.calls = 0
            damaged = ReturnAdjointTape.create(5, torch.device("cpu"), expected=record.values + .1)
            before = policy.action.detach().clone()
            with pytest.raises(RuntimeError, match="replay changed"):
                epoch(log_return_weights=weights, return_tape=damaged)
            assert optimizer.calls == scheduler.calls == 0
            torch.testing.assert_close(policy.action, before, atol=0, rtol=0)
    except BaseException as exc:
        results.put((rank, "error", f"{type(exc).__name__}: {exc}"))
        raise
    finally:
        dist.destroy_process_group()


@pytest.mark.parametrize('deferred',[False,True])
@pytest.mark.parametrize('recorded', [False, True])
def test_physical_return_adjoints_match_monolithic_accounts_on_two_ranks(tmp_path, monkeypatch, deferred, recorded):
    import stockagent.training.loss as loss_module
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_DEFER_VALIDATION','1' if deferred else '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_RECORDED_RETURN_CANDIDATE', '1' if recorded else '0')
    references = {}
    run = loss_module.run_backtest_torch
    captured = []
    def capture(*args, **kwargs):
        result = run(*args, **kwargs)
        captured.append(result.strategy_returns)
        return result
    monkeypatch.setattr(loss_module, "run_backtest_torch", capture)
    for objective in OBJECTIVES:
        captured.clear()
        split, runtime, loss_fn = annual_fixture()
        policy = PolicyWithUnused()
        optimizer = CountingSGD(policy.parameters())
        independent_year_reference(split, runtime, loss_fn, policy)
        direct = trajectory_metric_loss(torch.cat(captured), objective, [0, 2, 5])
        direct.backward()
        gradient = policy.action.grad.detach().clone()
        optimizer.step()
        references[objective] = (direct.item(), gradient, policy.action.detach().clone())
    context = mp.get_context("spawn")
    results = context.Queue()
    workers = [context.Process(target=_physical_worker, args=(rank, str(tmp_path / "init"), results)) for rank in range(2)]
    for worker in workers:
        worker.start()
    observed = [results.get(timeout=80) for _ in range(2 * len(OBJECTIVES))]
    for worker in workers:
        worker.join(timeout=15)
        if worker.is_alive():
            worker.terminate()
        assert worker.exitcode == 0
    for _, objective, result in observed:
        assert objective != "error", result
        value, gradient, parameter = references[objective]
        assert result["loss"] == pytest.approx(value, abs=1e-10)
        torch.testing.assert_close(torch.tensor(result["gradient"]), gradient, rtol=2e-5, atol=1e-7)
        torch.testing.assert_close(torch.tensor(result["parameter"]), parameter, rtol=2e-6, atol=1e-7)

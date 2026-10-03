"""Signed financial objectives need an absolute scheduler improvement test."""
import pytest
import torch

from stockagent.config import load_config
from stockagent.training.trainer import _create_lr_scheduler
from stockagent.training.checkpoint_contract import _active_scheduler_checkpoint_contract


def test_negative_loss_worsening_does_not_reset_plateau_and_minimum_lr_is_used():
    cfg = load_config('configs/markets/tw_futures_v8_margin_verified_2011_capital10m_tx_front_roll_feasible_gradient_v14.yaml')
    cfg.training.lr_scheduler = 'plateau'
    cfg.training.lr_scheduler_gamma = .5
    cfg.training.lr_scheduler_patience = 1
    cfg.training.lr_scheduler_threshold = .001
    cfg.training.lr_scheduler_eta_min = .01
    optimizer = torch.optim.SGD([torch.nn.Parameter(torch.ones(1))], lr=.08)
    scheduler, _, needs_metric, interval = _create_lr_scheduler(optimizer, cfg, steps_per_epoch=1)
    assert needs_metric and interval == 'epoch'
    scheduler.step(-1.)
    scheduler.step(-.9999)
    scheduler.step(-.9999)
    assert optimizer.param_groups[0]['lr'] == pytest.approx(.04)
    assert scheduler.best == -1.
    for _ in range(20):
        scheduler.step(-.9999)
    assert optimizer.param_groups[0]['lr'] == .01
    contract = _active_scheduler_checkpoint_contract(cfg)
    assert contract['threshold_mode'] == 'abs'
    assert contract['min_lr'] == .01
    # A real absolute improvement resets the no-improvement counter.
    scheduler.step(-1.002)
    assert scheduler.best == -1.002
    assert scheduler.num_bad_epochs == 0

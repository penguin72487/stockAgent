#!/usr/bin/env python3
"""Replay the dated ODF correction and compare ledger paths; never train a model."""
from __future__ import annotations

import argparse
from dataclasses import fields
from datetime import date
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import polars as pl
import torch

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch as run
from stockagent.data import tw_futures_margin as m


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--old-source', type=Path, required=True)
    parser.add_argument('--old-release', type=Path, required=True)
    parser.add_argument('--new-release', type=Path, required=True)
    parser.add_argument('--review', type=Path, required=True)
    args = parser.parse_args()
    torch.set_num_threads(2)
    review = json.loads(args.review.read_text())
    old_daily = args.old_release / 'daily/continuous_daily.parquet'
    new_daily = args.new_release / 'daily/continuous_daily.parquet'
    assert sha256_file(old_daily) == sha256_file(new_daily), 'price/universe bytes changed'
    payload = np.load(args.evidence_dir / 'critical_tape.npz')
    dates, original = payload['dates'], payload['execution']
    corrected = original.copy()
    affected = (
        pl.col('product').is_in(review['products'])
        & pl.col('date').is_between(date.fromisoformat(review['start']), date.fromisoformat(review['end']))
        & pl.col('date').is_between(date.fromisoformat(str(dates[0])), date.fromisoformat(str(dates[-1])))
    )
    rules = pl.scan_parquet(args.new_release / 'rules/rules.parquet').filter(affected).select(
        'date', 'physical_contract', 'position_limit', 'second_position_limit'
    )
    slots = pl.scan_parquet(new_daily).filter(affected).select('date', 'physical_contract', 'portfolio_slot')
    changes = rules.join(slots, on=['date', 'physical_contract'], how='inner', validate='1:1').collect()
    for row in changes.iter_rows(named=True):
        i = int(np.searchsorted(dates, np.datetime64(row['date'])))
        slot = int(row['portfolio_slot']) - 1
        for field, name in ((m.POSITION_LIMIT, 'position_limit'), (m.SECOND_POSITION_LIMIT, 'second_position_limit')):
            assert original[i, slot, field] == np.float32(review['incorrect_cap'])
            corrected[i, slot, field] = row[name]
    other_fields = [k for k in range(original.shape[-1]) if k not in (m.POSITION_LIMIT, m.SECOND_POSITION_LIMIT)]
    assert np.array_equal(original[..., other_fields], corrected[..., other_fields], equal_nan=True)
    np.savez_compressed(args.evidence_dir / 'critical_tape_corrected.npz',
                        dates=dates, execution=corrected, mask=payload['mask'])
    j = int(np.flatnonzero(dates == np.datetime64('2015-10-08'))[0])
    events = slots.filter(pl.col('date') == date(2015, 10, 8)).collect().sort('portfolio_slot')
    slot = int(events['portfolio_slot'][0]) - 1
    # CARRY_SOURCE_SLOT stores source slot + 1; zero means no transfer.
    origin = int(original[j, slot, m.CARRY_SOURCE_SLOT]) - 1
    replay = []
    for sign in (-1., 1.):
        initial_q = torch.zeros(original.shape[1])
        initial_q[origin] = sign * 3
        for version, tape in [('old', original), ('reviewed', corrected)]:
            x = torch.from_numpy(tape[j:j+1])
            w = torch.zeros((1, original.shape[1]), requires_grad=True)
            result = run(w, x, initial_capital=100000000., initial_quantities=initial_q,
                         recoverable_backward=True)
            result.strategy_returns.sum().backward()
            entry = dict(version=version, sign=sign, initial_contracts=sign*3,
                         default_reason=int(result.default_reason_history.item()),
                         alive=bool(result.final_alive),
                         actual_nav=float(result.margin_audit_history[0, 2]),
                         terminal_equity_scale=float(result.final_equity_scale),
                         log_return=float(result.strategy_returns.detach().sum()),
                         gradient_finite=bool(torch.isfinite(w.grad).all()))
            assert entry['gradient_finite']
            assert entry['default_reason'] == (4 if version == 'old' else 0)
            assert entry['alive'] == (version == 'reviewed')
            if version == 'reviewed':
                assert np.isclose(entry['log_return'], np.log(entry['actual_nav']/1e8), atol=2e-7)
            replay.append(entry)
    # Compare every reported field on identical corrected tape. Only the
    # terminal-failure derivative changed, never the authoritative forward.
    name = '_fold14_original_margin_ledger'
    spec = importlib.util.spec_from_file_location(name, args.old_source / 'stockagent/backtest/tw_futures_portfolio.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    old_run = module.run_tw_futures_portfolio_integer_torch
    x = torch.from_numpy(corrected)
    # A small signed feasible request covers entry, carry, fees and corporate transfer.
    torch.manual_seed(17)
    w = (torch.randn(x.shape[:2]) * .00005 * torch.from_numpy(payload['mask'])).requires_grad_()
    a = old_run(w, x, initial_capital=1e8, recoverable_backward=True)
    a.strategy_returns.sum().backward()
    old_gradient = w.grad.clone()
    w.grad = None
    b = run(w, x, initial_capital=1e8, recoverable_backward=True)
    b.strategy_returns.sum().backward()
    checked = []
    for field in fields(b):
        left, right = getattr(a, field.name), getattr(b, field.name)
        if isinstance(left, torch.Tensor):
            torch.testing.assert_close(left, right, rtol=0, atol=0, equal_nan=True)
            checked.append(field.name)
    assert not b.default_history.any(), 'normal-account gradient parity requires a solvent path'
    torch.testing.assert_close(old_gradient, w.grad, rtol=0, atol=0)
    receipt = dict(
        engineering_validation_only=True, formal_training_started=False,
        daily_price_universe_bytes_equal=True, daily_sha256=sha256_file(new_daily),
        only_cap_tape_channels_changed=True, corrected_contract_days=changes.height,
        critical_date='2015-10-08', successor_slot=slot, source_slot=origin,
        three_contract_replays=replay, identical_tape_forward_parity_fields=checked,
        solvent_gradient_bitwise_equal=True,
        original_tape_sha256=sha256_file(args.evidence_dir/'critical_tape.npz'),
        corrected_tape_sha256=sha256_file(args.evidence_dir/'critical_tape_corrected.npz'),
    )
    atomic_write_json(args.evidence_dir / 'replay_validation.json', receipt)
    print(json.dumps(receipt, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

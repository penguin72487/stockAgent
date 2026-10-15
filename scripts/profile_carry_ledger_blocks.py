"""Measure the canonical ledger with real saved actions; no optimizer update."""
from __future__ import annotations

import argparse
from dataclasses import fields
from datetime import timedelta
import json
import os
from pathlib import Path
import statistics
import sys
import time
from types import SimpleNamespace


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--code-root', type=Path, required=True)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--actions', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--rows', type=int, default=64)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--trace', action='store_true')
    p.add_argument('--compare-canonical', action='store_true')
    p.add_argument('--event-compression', action='store_true')
    a = p.parse_args()
    sys.path.insert(0, str(a.code_root.resolve(strict=True)))
    import numpy as np
    import torch
    from downloader.artifact_io import atomic_write_json
    from stockagent.config import load_config
    from stockagent.data.panel import build_panel
    from stockagent.data.tw_day_trade_carry_source import build_prepared_day_trade_carry_source
    from stockagent.backtest.tw_day_trade_carry import (
        get_day_trade_carry_compile_stats, run_day_trade_carry_sessions,
    )
    from stockagent.runtime_identity import verify_source_release
    from stockagent.storage_layout import admit_output
    from stockagent.training.day_trade_carry_bridge import PreparedDayTradeCarryBatch
    from stockagent.training.trainer import _build_execution_runtime
    from train import _build_panel_kwargs, _configure_cpu_parallelism

    rank = int(os.environ['LOCAL_RANK'])
    assert a.rows > 0 and a.repeats >= 3 and torch.cuda.device_count() == 2
    torch.cuda.set_device(rank)
    torch.distributed.init_process_group('nccl', timeout=timedelta(minutes=30))
    assert torch.distributed.get_world_size() == 2
    _configure_cpu_parallelism(cpu_threads=8, compile_threads=8, local_world_size=1)
    device = torch.device('cuda', rank)
    output = admit_output(a.output / f'rank_{rank}', 'benchmark')
    output.mkdir(parents=True, exist_ok=False)
    identity = verify_source_release(Path(os.environ['STOCKAGENT_CODE_RELEASE_RECEIPT']), a.code_root)
    cfg = load_config(a.config)
    panel = build_panel(cfg.data.parquet_root, **_build_panel_kwargs(cfg))
    source = build_prepared_day_trade_carry_source(
        panel=panel, minute_root=cfg.data.day_trade_minute_execution_root,
        public_feature_path=cfg.data.day_trade_physical_public_feature_path or cfg.data.tw_public_feature_path,
        cache_dir=cfg.data.day_trade_minute_execution_cache_dir,
        allow_daily_proxy=cfg.data.day_trade_minute_execution_allow_daily_proxy,
        daily_proxy_price_policy=cfg.data.day_trade_minute_execution_daily_proxy_price_policy,
        corporate_action_mode=cfg.trading.tw_corporate_action_mode,
        subscription_right_policy=cfg.trading.tw_day_trade_subscription_right_policy,
        entry_remainder_policy=cfg.trading.tw_day_trade_entry_remainder_policy,
        terminal_liquidation_unlimited_capacity=cfg.trading.tw_day_trade_terminal_liquidation_unlimited_capacity,
        sparse_event_slots=None)
    with np.load(a.actions, allow_pickle=False) as data:
        symbols = tuple(str(s) for s in data['carry_universe'])
        assert symbols == tuple(panel.symbols)
        dates = np.asarray(data['dates'][:a.rows], dtype='datetime64[D]')
        saved = np.array(data['requested_weights_history'][:a.rows], copy=True)
    assert len(saved) == a.rows
    lookup = {str(d): i for i, d in enumerate(np.asarray(panel.dates, dtype='datetime64[D]'))}
    rows = [lookup[str(d)] for d in dates]
    assert all(x + 1 == y for x, y in zip(rows, rows[1:]))
    start = time.perf_counter()
    class ObservedSplit(SimpleNamespace):
        def __len__(self):return len(self._valid_indices_cpu)
    split = ObservedSplit(features=panel.features, _valid_indices_cpu=torch.tensor(rows),
        sample_mask=None, symbol_indices=None, execution_mode='tw_day_trade')
    staged = source.prepare_batch(split, 0, len(rows), event_compression=a.event_compression)
    cpu_stage_s = time.perf_counter() - start
    torch.cuda.synchronize()
    start = time.perf_counter()
    sessions = staged.sessions(device)
    torch.cuda.synchronize()
    transport_s = time.perf_counter() - start
    physical_bytes = sum(t.numel() * t.element_size() for t in staged.tensors.values())
    source_shapes = {name: {'shape': list(value.shape), 'dtype': str(value.dtype)}
                     for name, value in staged.tensors.items()}
    transport_records=[]
    if int(os.environ.get('STOCKAGENT_DAY_TRADE_PREPARED_CACHE_BYTES','0'))>0:
        for _ in range(3):
            start=time.perf_counter()
            staged=source.prepare_batch(split,0,len(rows),event_compression=a.event_compression)
            prepare_s=time.perf_counter()-start
            torch.cuda.synchronize();start=time.perf_counter()
            del sessions
            sessions=staged.sessions(device)
            torch.cuda.synchronize()
            transport_records.append(dict(prepare_s=prepare_s,transport_s=time.perf_counter()-start))
    runtime = _build_execution_runtime(panel, cfg, device)
    weights = torch.tensor(saved, device=device, dtype=torch.float32)
    # Saved actions already include the canonical causal opening mask. This
    # fixed-action raw-ledger probe does not derive a new trading policy.
    common = dict(can_enter=torch.ones_like(weights, dtype=torch.bool),
        buy_fee_rate=runtime.buy_fee_rates.double(),
        day_sell_fee_rate=runtime.sell_fee_rates.double(),
        normal_sell_fee_rate=runtime.normal_sell_fee_rates.double(),
        rebate_rate=runtime.commission_rebate_rates.double(),
        initial_capital=float(cfg.trading.volume_participation_equity),
        event_compression=a.event_compression, require_minute_nav_grad=False)
    records = []
    reference = None
    oracle_receipt = None
    if a.compare_canonical:
        selected = os.environ.get('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH', '0')
        os.environ['STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH'] = '0'
        oracle_w = weights.detach().clone().requires_grad_()
        with torch.enable_grad():
            oracle = run_day_trade_carry_sessions(oracle_w, sessions, **common)
            oracle_grad = torch.autograd.grad(-oracle.strategy_returns.mean(), oracle_w)[0]
        reference = [v.detach().clone() for v in (
            oracle.strategy_returns, oracle.turnovers, oracle.shares_history,
            oracle.minute_nav, oracle.final_state.last_nav)]
        oracle_receipt = dict(grad_l1=float(oracle_grad.abs().sum()), full_forward_checked=True)
        os.environ['STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH'] = selected
        del oracle, oracle_w
    for mode in ['train', 'eval']:
        for repeat in range(a.repeats + 2):
            w = weights.detach().clone().requires_grad_(mode == 'train')
            torch.cuda.synchronize()
            before = get_day_trade_carry_compile_stats()
            torch.cuda.reset_peak_memory_stats()
            start = time.perf_counter()
            with torch.set_grad_enabled(mode == 'train'):
                result = run_day_trade_carry_sessions(w, sessions, **common)
                loss = -result.strategy_returns.mean()
            torch.cuda.synchronize()
            forward_s = time.perf_counter() - start
            backward_s = 0.0
            if mode == 'train':
                start = time.perf_counter()
                loss.backward()
                torch.cuda.synchronize()
                backward_s = time.perf_counter() - start
                assert torch.isfinite(w.grad).all() and float(w.grad.abs().sum()) > 0
                if a.compare_canonical:
                    torch.testing.assert_close(w.grad, oracle_grad, rtol=2e-4, atol=1e-8)
            assert torch.isfinite(result.strategy_returns).all()
            assert bool(result.final_state.inventory.failed == 0)
            assert bool(result.final_state.alive)
            signature = [result.strategy_returns, result.turnovers, result.shares_history,
                         result.minute_nav, result.final_state.last_nav]
            if reference is None:
                reference = [v.detach().clone() for v in signature]
            else:
                for actual, expected in zip(signature, reference):
                    torch.testing.assert_close(actual, expected, rtol=1e-9, atol=1e-7)
            record = dict(mode=mode, repetition=repeat, warmup=repeat < 2,
                forward_s=forward_s, backward_s=backward_s, total_s=forward_s + backward_s,
                peak_allocated_bytes=torch.cuda.max_memory_allocated(),
                grad_l1=float(w.grad.abs().sum()) if w.grad is not None else None,
                compile_delta={k: v - before[k] for k, v in get_day_trade_carry_compile_stats().items()})
            if os.environ.get('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH') == '1':
                from stockagent.backtest.tw_day_trade_graph import get_explicit_carry_graph_stats
                record['explicit_graph_stats'] = get_explicit_carry_graph_stats()
            records.append(record)
            atomic_write_json(output / 'progress.json', records)
            print(json.dumps({'rank': rank, **record}), flush=True)
            del result, loss, w
        if a.trace:
            activities = [torch.profiler.ProfilerActivity.CPU, torch.profiler.ProfilerActivity.CUDA]
            w = weights.detach().clone().requires_grad_(mode == 'train')
            with torch.profiler.profile(activities=activities, record_shapes=True) as profiler:
                with torch.set_grad_enabled(mode == 'train'):
                    with torch.profiler.record_function('canonical_financial_block_forward'):
                        result = run_day_trade_carry_sessions(w, sessions, **common)
                        loss = -result.strategy_returns.mean()
                    if mode == 'train':
                        with torch.profiler.record_function('canonical_financial_block_backward'):
                            loss.backward()
                torch.cuda.synchronize()
            profiler.export_chrome_trace(str(output / f'{mode}-trace.json'))
            (output / f'{mode}-cpu.txt').write_text(profiler.key_averages().table(sort_by='self_cpu_time_total', row_limit=25))
            (output / f'{mode}-cuda.txt').write_text(profiler.key_averages().table(sort_by='self_cuda_time_total', row_limit=25))
            del result, loss, w
    atomic_write_json(output / 'profile.json', dict(
        contract='canonical_real_action_financial_block_profile_v1', source=identity,
        rank=rank, world_size=2, rows=len(rows), symbols=len(symbols),
        event_compression=a.event_compression,
        dates=[str(dates[0]), str(dates[-1])], cpu_stage_s=cpu_stage_s,
        transport_s=transport_s, transport_payload_bytes=physical_bytes,
        transport_records=transport_records, prepared_cache=source.prepared_batch_cache_info(),
        source_shapes=source_shapes, source_audit=source.audit_receipt,
        canonical_oracle=oracle_receipt,
        records=records, formal_optimizer_updates=0, full_fold_acceptance=False,
        warm_median_s={mode: statistics.median(r['total_s'] for r in records
                           if r['mode'] == mode and not r['warmup']) for mode in ['train', 'eval']}))
    torch.distributed.barrier()
    torch.distributed.destroy_process_group()


if __name__ == '__main__':
    main()

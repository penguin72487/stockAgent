"""Read-only comparison of canonical training and 09:00 inference inputs.

Uses one already-validated cache generation. It does not claim that revised
live data equal an older release, or that a historical fill is a broker fill.
"""
from __future__ import annotations

import argparse
from dataclasses import fields, replace
import hashlib
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import numpy as np
import torch

from stockagent.config import load_config
from stockagent.data.panel import _append_day_trade_open_gap_feature, _panel_from_cache_payload
from stockagent.data.panel_cache import load_panel_cache_v2, read_panel_cache_v2_meta
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.live.signal_engine import _day_trade_live_model_window, _day_trade_model_eligibility
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.windowed import dataset_to_windowed_tensors


def audit(config_path: str, dates: list[str]) -> dict:
    config = load_config(config_path)
    cache = Path(config.data.panel_cache_root)
    meta = read_panel_cache_v2_meta(cache)
    if meta is None:
        raise ValueError("run the exact configuration's data preflight first")
    panel = _panel_from_cache_payload(load_panel_cache_v2(cache,
        source_hash=meta['source_hash'], backend_key=meta['backend_key'], version=meta['version']))
    lookback = int(config.training.lookback)
    checks = []
    for day in dates:
        found = np.flatnonzero(panel.dates.astype('datetime64[D]') == np.datetime64(day))
        if len(found) != 1 or int(found[0]) < lookback:
            raise ValueError(f"date lacks exact history: {day}")
        index = int(found[0])
        # Bound allocations to one lookback, preserving every symbol/feature.
        updates = {f.name: value[index-lookback:index+1].copy()
                   for f in fields(panel)
                   if isinstance(value := getattr(panel, f.name), np.ndarray)
                   and value.ndim and value.shape[0] == panel.num_dates}
        small = replace(panel, **updates, content_fingerprints=None)
        small = _append_day_trade_open_gap_feature(small)
        dataset = CrossSectionalDataset(small, np.array([lookback]), lookback,
            execution_mode='tw_day_trade', short_capacity_limit_enabled=False,
            day_trade_unlimited_margin_conversion=True,
            tw_corporate_action_mode=config.trading.tw_corporate_action_mode,
            tw_commission_rebate_timing=config.trading.tw_commission_rebate_timing)
        batch = dataset_to_windowed_tensors(dataset).batch_by_rows(0, 1, torch.device('cpu'), False)
        opening = small.open_prices[-1].astype(np.float64)
        snapshot = PriceSnapshot(prices=opening, open_prices=opening,
            source='verified_historical_open_audit', timestamp=f'{day}T09:00:00+08:00',
            available_count=int(np.isfinite(opening).sum()))
        live, cutoff, _, _, _ = _day_trade_live_model_window(small,
            panel_idx=lookback-1, lookback=lookback, price_snapshot=snapshot,
            resolved_asof=f'{day} 09:00:00', source_timezone='Asia/Taipei')
        mask, _, _ = _day_trade_model_eligibility(small, session_date=day,
                                                 parquet_root=config.data.parquet_root)
        trained = batch['x'][0].numpy()
        np.testing.assert_array_equal(live, trained)
        np.testing.assert_array_equal(mask, batch['tradable_mask'][0].numpy())
        checks.append(dict(date=day, feature_cutoff=cutoff, shape=list(live.shape),
            input_sha256=hashlib.sha256(live.tobytes()).hexdigest(),
            eligible_symbols=int(mask.sum()), max_abs_input_difference=0.))
    return dict(kind='same_release_training_web_input_parity', status='passed',
        config=config_path, cache_generation=meta['generation'], source_hash=meta['source_hash'],
        checks=checks, scope='input_window_and_policy_mask_only',
        excludes=['mutable_live_source_equality', 'broker_fill_equality', 'model_quality'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--date', action='append', required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.config, args.date), ensure_ascii=False, indent=2))

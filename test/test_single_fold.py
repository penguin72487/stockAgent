#!/usr/bin/env python3
"""Test script: train on single fold to verify all optimizations."""

from pathlib import Path
import torch
import sys

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.training.trainer import run_training

def main():
    config_path = "configs/experiment_baseline.yaml"
    if not Path(config_path).exists():
        print(f"ERROR: {config_path} not found")
        sys.exit(1)
    
    print("[TEST] Loading config...")
    config = load_config(config_path)
    
    print("[TEST] Building panel...")
    panel = build_panel(
        config.data.parquet_root,
        use_rapids=config.data.use_rapids,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tradable_mode=config.data.tradable_mode,
        trading_volume_policy=config.data.trading_volume_policy,
        security_filter=config.data.security_filter,
        strict_no_fallback=config.training.strict_no_fallback,
        panel_backend=config.data.panel_backend,
        panel_load_workers=config.data.panel_load_workers,
        external_feature_path=(
            config.data.tw_public_feature_path if config.data.use_tw_public_features else None
        ),
        external_market_symbol=config.data.tw_public_market_symbol,
    )
    print(f"  Panel shape: {panel.features.shape} (T={panel.num_dates}, S={panel.num_symbols}, F={len(panel.feature_names)})")
    
    print("[TEST] Building walk-forward folds...")
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
    )
    folds = list(folds)
    print(f"  Total folds: {len(folds)}")
    
    # Test only first fold
    print("\n[TEST] Training on first fold...")
    single_fold = [folds[0]]
    
    output_dir = Path("artifacts_test")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        results = run_training(panel, single_fold, config, output_dir)
        print(f"\n[TEST] SUCCESS: First fold trained")
        print(f"  Result: fold_id={results[0].fold_id}, best_val_loss={results[0].best_val_loss:.6f}")
        print(f"  Test Sharpe: {results[0].test_metrics.get('sharpe', 'N/A')}")
    except Exception as e:
        print(f"\n[TEST] FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

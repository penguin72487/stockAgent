from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import torch

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.trainer import _dataset_to_tensors, _tensor_nbytes
from stockagent.training.windowed import dataset_to_windowed_tensors


def _gb(num_bytes: int) -> float:
    return float(num_bytes) / 1024**3


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark materialized vs lazy windowed tensor setup.")
    parser.add_argument("--config", default="configs/experiment_baseline.yaml")
    parser.add_argument("--fold-index", type=int, default=0)
    parser.add_argument("--lookback", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--batches", type=int, default=20)
    parser.add_argument("--device", default="cpu")
    parser.add_argument("--cache-on-device", action="store_true")
    parser.add_argument("--skip-materialized", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    lookback = int(args.lookback) if args.lookback is not None else int(config.training.lookback)
    device = torch.device(args.device)
    panel = build_panel(
        config.data.parquet_root,
        use_rapids=config.data.use_rapids,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tradable_mode=config.data.tradable_mode,
        trading_volume_policy=config.data.trading_volume_policy,
        strict_no_fallback=config.training.strict_no_fallback,
        panel_backend=config.data.panel_backend,
        panel_load_workers=config.data.panel_load_workers,
    )
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    fold = folds[min(args.fold_index, len(folds) - 1)]
    dataset = CrossSectionalDataset(panel, fold.train_indices, lookback)
    configured_batch = int(args.batch_size) if args.batch_size is not None else int(config.training.batch_size_train)
    batch_size = max(1, min(configured_batch, len(dataset)))

    materialized_s: float | None = None
    materialized_bytes: int | None = None
    if not args.skip_materialized:
        materialized_start = time.perf_counter()
        materialized = _dataset_to_tensors(dataset)
        materialized_s = time.perf_counter() - materialized_start
        materialized_bytes = sum(_tensor_nbytes(tensor) for tensor in materialized)

    windowed_start = time.perf_counter()
    windowed = dataset_to_windowed_tensors(dataset)
    if args.cache_on_device and device.type != "cpu":
        windowed = windowed.to_device_cache(device)
    windowed_s = time.perf_counter() - windowed_start
    base_tensors = (
        windowed.features,
        windowed.valid_indices,
        windowed.future_log_returns,
        windowed.tradable_mask,
        windowed.can_buy_mask,
        windowed.can_sell_mask,
        windowed.benchmark,
    )
    windowed_bytes = sum(_tensor_nbytes(tensor) for tensor in base_tensors)

    gather_start = time.perf_counter()
    rows = len(windowed)
    batches = min(max(1, int(args.batches)), max(1, (rows + batch_size - 1) // batch_size))
    for batch_idx in range(batches):
        start = (batch_idx * batch_size) % rows
        end = min(start + batch_size, rows)
        _ = windowed.batch_by_rows(start, end, device, non_blocking=(device.type == "cuda"))
        if device.type == "cuda":
            torch.cuda.synchronize(device)
    gather_s = time.perf_counter() - gather_start

    print(
        {
            "fold_id": fold.fold_id,
            "lookback": lookback,
            "rows": len(dataset),
            "symbols": panel.num_symbols,
            "features": len(panel.feature_names),
            "batch_size": batch_size,
            "device": str(device),
            "cache_on_device": bool(args.cache_on_device and device.type != "cpu"),
            "materialized_setup_s": None if materialized_s is None else round(materialized_s, 4),
            "materialized_gb": None if materialized_bytes is None else round(_gb(materialized_bytes), 4),
            "windowed_setup_s": round(windowed_s, 4),
            "windowed_base_gb": round(_gb(windowed_bytes), 4),
            "windowed_gather_s_per_batch": round(gather_s / batches, 6),
        },
        flush=True,
    )


if __name__ == "__main__":
    main()

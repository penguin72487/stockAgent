# Windowed Tensor Pipeline

This project defaults to lazy windowed tensors for neural training when the loss
objective is compatible with canonical tensor backtesting.

Default config:

```yaml
training:
  materialize_window_tensors: false
  compile_loss: true
data:
  panel_backend: auto
  panel_load_workers: 4
```

## Why

The old tensor path materialized all lookback windows as:

```text
[rows, lookback, symbols, features]
```

For lookback 32 and a full symbol universe, that multiplies panel memory by
the lookback length. The lazy path stores the base panel tensors:

```text
features: [dates, symbols, features]
valid_indices: [rows]
```

Each training/eval batch gathers only the requested windows. The last training
batch is padded by repeating the final valid row and marking those extra rows as
`sample_mask=false`, so `torch.compile(dynamic=False)` can keep a stable batch
shape without changing the loss.

## Benchmark Helper

Run:

```bash
/home/user/miniforge3/envs/fintech/bin/python scripts/benchmark_windowed_pipeline.py \
  --config configs/experiment_baseline.yaml
```

The helper compares materialized setup time and memory against lazy windowed
setup and per-batch gather time. It does not change the training config.

## Guardrails

- Training, validation, sampled test loss, and final test evaluation still use
  the canonical tensor backtest.
- Portfolio state is carried across chunks and reset only at fold/segment
  boundaries.
- Return-series losses keep batch order sequential.
- Special rank-objective eval paths keep the existing materialized tensor path
  until they are separately refactored.

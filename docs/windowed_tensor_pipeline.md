# Windowed Tensor Pipeline

Neural training always starts from lazy `WindowedSplitTensors`. There is no
configuration switch that can select the removed persistent materialized-window
executor.

Default config:

```yaml
training:
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
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_windowed_pipeline.py \
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
- Contiguous batches use the compile-friendly panel-slab forward. A guarded
  per-batch window gather remains only for models or batch shapes that cannot
  satisfy the slab contract; it does not materialize and retain the full split.
- Rank and auxiliary objectives still consume the same lazy model-forward path;
  only their objective reduction may require collecting all fold outputs.

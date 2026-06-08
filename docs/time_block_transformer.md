# Time-Block Causal Transformer

This path is for long-lookback Transformer portfolio training without repeatedly
materializing sliding windows.

## Core Idea

Legacy tensor training builds repeated windows with shape `[N, lookback, S, F]`.
For long lookbacks this duplicates most historical rows many times.

Time-block training keeps base tensors:

- features: `[T, S, F]`
- returns/masks: `[T, S]`
- benchmark: `[T]`

Each batch chooses target days `[a, b)` and context
`[a - lookback + 1, b)`. The model runs temporal Transformer blocks once on
that context and emits weights only for the target days.

## Causality

Temporal attention uses a local causal mask:

- target day `t` can attend to `t`
- target day `t` can attend to the previous `lookback - 1` days
- target day `t` cannot attend to future days

Cross-sectional attention is applied only within the same target day.

## Config

Use:

```bash
/home/user/miniforge3/envs/fintech/bin/python train.py \
  --config configs/experiment_timeblock_transformer_logutil.yaml
```

Important fields:

- `training.model_name: time_block_transformer_base_portfolio`
- `training.time_block_training: true`
- `training.materialize_window_tensors: false`
- `training.loss_type: log_utility`
- `training.lookback: 256`
- `training.target_block_size: 64`
- `training.eval_target_block_size: 256`
- `training.transformer_base_portfolio.use_time_pos: false`
- `training.transformer_base_portfolio.temporal_causal: true`
- `training.transformer_base_portfolio.temporal_local_window: 256`

## Benchmarking

The helper prints benchmark commands and summarizes existing epoch curves:

```bash
/home/user/miniforge3/envs/fintech/bin/python scripts/benchmark_timeblock_transformer.py \
  --baseline-curve artifacts/<baseline>/epoch_curve.jsonl \
  --timeblock-curve artifacts/<timeblock>/epoch_curve.jsonl
```

Do not claim a speedup until both runs have measured `epoch_wall_s` in
`epoch_curve.jsonl`.

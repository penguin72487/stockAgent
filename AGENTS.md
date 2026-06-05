# AGENTS.md

This file is the persistent operating contract for future coding agents working
in this repository. Read it before changing code, configs, training logic, model
architecture, or explainability artifacts.

## Communication

- Reply to the user in Traditional Chinese unless they explicitly request another language.
- Be direct and implementation-oriented. The user usually expects code changes, not only proposals.
- Preserve user intent across turns. In this project, "do not skip" means compute and profile the full epoch workflow unless the user explicitly changes that requirement.
- When explaining trading/model changes, separate engineering facts from investment interpretation.

## Workspace And Environment

- Repo root: `/home/user/stockAgent`.
- Preferred Python runtime:
  - `/home/user/miniforge3/envs/fintech/bin/python`
- Do not assume `python` exists on PATH. Use the explicit fintech Python path for checks and tests.
- CUDA is expected for training. If CUDA is unavailable and `runner.require_cuda` is true, do not silently fall back to CPU.
- Use `rg` / `rg --files` for search.
- Use `apply_patch` for manual file edits.
- Do not revert user changes or unrelated dirty files.
- Do not use destructive git commands such as `git reset --hard` or `git checkout --` unless the user explicitly asks.

## Current Baseline Precision Contract

The baseline should use BF16 AMP, not FP16.

Required config state:

```yaml
environment:
  device: cuda
  use_tensor_cores: true
  amp_dtype: bf16

training:
  prefer_fp16: false
```

Implementation expectations:

- `stockagent.training.trainer._resolve_amp_dtype("bf16")` must resolve to `torch.bfloat16`.
- Main train/eval/profile model forward and loss computation should run inside `_autocast_context(device, amp_dtype)`.
- BF16 AMP should leave `GradScaler` disabled. `GradScaler` is only for FP16:
  - `GradScaler(enabled=device.type == "cuda" and amp_dtype == torch.float16)`
- It is normal and desirable that some tensors remain FP32:
  - model parameters
  - input storage tensors
  - portfolio weights after softmax
  - loss/backtest accumulation and numerically sensitive finance metrics
- Do not force the entire pipeline to permanent BF16 storage just to satisfy "BF16"; use AMP for compute and keep sensitive reductions stable.

## Current Main Model Contract

The active model is `transformer_base_portfolio`.

The active Transformer-base lookback-32 config is:

```yaml
training:
  model_name: transformer_base_portfolio
  lookback: 32
  loss_type: sortino

  transformer_base_portfolio:
    d_model: 48
    attention_mode: latent
    use_flash_attention: true
    use_time_pos: true
    use_symbol_pos: true
    input_dropout: 0.0
    temporal_layers: 2
    temporal_heads: 4
    temporal_ffn_mult: 2
    temporal_pooling: attention
    cross_layers: 1
    cross_heads: 4
    cross_ffn_mult: 2
    joint_layers: 2
    joint_heads: 4
    joint_ffn_mult: 2
    latent_layers: 1
    num_latent_factors: 16
    num_market_tokens: 4
    market_layers: 1
    head_hidden_dim: 48
    head_layers: 1
    dropout: 0.2
    default_temperature: 1.0
    portfolio_mode: long_short
    max_full_tokens: 4096
    checkpoint_blocks: false
    return_aux: true
    return_aux_details: false
```

Notes:

- The scalable Transformer can be moved from complete to compact via `attention_mode`.
- Avoid `attention_mode: full` on a full market universe unless symbol count is small enough for the `max_full_tokens` guard.
- For large universes, prefer `latent` or `market_token`.
- `return_aux_details` is useful for explainability but can increase memory pressure during training. Prefer `false` for tight VRAM training and enable it for explainability runs when needed.
- The previous low-rank model remains available as `low_rank_market_transformer_portfolio`.

## Scalable Transformer Base Portfolio

The project also has `transformer_base_portfolio`, a configurable Transformer
family that can move from complete to compact by changing config only.

Key switch:

```yaml
training:
  model_name: transformer_base_portfolio
  transformer_base_portfolio:
    attention_mode: latent
```

Modes:

- `full`: joint attention over all `lookback * stocks` tokens. Most complete, O((L*S)^2), use only for small universes or debug subsets.
- `axial`: temporal attention per stock, then cross-stock attention per day. O(S*L^2 + L*S^2).
- `latent`: temporal attention, then latent factors and market tokens. Large-universe friendly default.
- `market_token`: temporal attention, then market-token bottleneck. Smaller than latent.
- `temporal_only`: no cross-stock attention. Smallest Transformer baseline.

Rules:

- Keep `use_flash_attention: true` unless debugging. The implementation uses PyTorch SDPA so CUDA can select flash/memory-efficient kernels when shape and dtype allow it.
- Do not assume Flash Attention removes full attention compute cost. It reduces memory pressure, but `full` mode is still quadratic in `lookback * stocks`.
- Use `max_full_tokens` as an OOM guard for `full` mode.
- Prefer `latent` or `market_token` for full market universes.
- Use `d_model`, layer counts, heads, latent factors, market tokens, and `attention_mode` as the main knobs for scaling small to complete.

## Portfolio Direction Intent

The active baseline should support long/short portfolio weights when the user asks
for multi-directional trading. Earlier experiments used long-only; always follow
the latest explicit user intent and keep model, loss, and backtest settings
aligned.

Guidelines:

- Current active low-rank baseline preference: `portfolio_mode: long_short`.
- Keep `trading.long_only: false` when the model is intended to do long/short.
- If changing `trading.long_only`, understand that it affects loss/backtest interpretation, not just the model head.
- Keep model output mode, loss assumptions, backtest assumptions, and report wording aligned. If they disagree, flag it explicitly.
- Rank-only loss can over-concentrate positions. If using rank objectives, keep turnover/concentration/backtest regularization in mind.
- If the user switches back to only-long behavior, change both the model direction mode and the loss/backtest direction assumptions deliberately and report the change.

## Canonical Tensor Backtest And Loss

The project goal is to keep train, validation, test, and inference return logic consistent and tensor-friendly.

Rules:

- Do not fork separate train/inference return formulas.
- Prefer the canonical tensor backtest in `stockagent/backtest/simulator.py` and loss integration in `stockagent/training/loss.py`.
- Keep computations GPU/tensor-friendly where possible.
- Do not move portfolio state to CPU between batches/chunks.
- Cross-batch/chunk portfolio state should be detached and cloned on GPU:
  - `t.detach().clone(memory_format=torch.contiguous_format)`
- `initial_weights` is trading state, not a gradient path across batches.
- If compiled loss hits CUDA Graph overwritten-output errors, only fall back the loss wrapper to eager tensor loss; do not disable model `torch.compile` globally.

## Epoch-Level Timing And Throughput

The user cares about total epoch wall time, not only train step time.

Rules:

- Use `epoch_curve.jsonl` when optimizing epoch-level speed.
- Break down "other" time before optimizing blindly.
- Every epoch should account for train, validation, sampled test loss, curve test, curve plot, checkpoint, scheduler/progress, and any reporting work.
- Do not hide expensive work behind `val_interval_epochs > 1` or skip curve/test/plot work unless the user explicitly asks.
- Recent preference: sampled test loss only needs one fold per epoch to reduce epoch-level overhead.
- Keep curve plotting async where possible.
- Keep GPU tensor caches disabled by default if VRAM is tight:
  - `cache_train_tensors_on_gpu: false`
  - `cache_eval_tensors_on_gpu: false`

## Feature Engineering Guardrails

Explainability indicated suspicious dependence on raw price level and raw liquidity.

Rules:

- Avoid feeding raw OHLC price levels directly when the goal is cross-stock generalization.
- Prefer log returns, relative price ratios, rolling normalization, and engineered K-line/volume features.
- If changing feature schema, update cache/versioning so stale panel caches are not reused.
- Keep `return_1d`, tradable masks, TW limit guards, and benchmark construction aligned with the canonical backtest.

## Explainability Contract

The user wants detailed model explainability to detect strange rules and judge strategy trustworthiness.

Expected explainability workflow:

- `python explain_model.py` should default to drawing the full explainability set unless the user asks for a smaller run.
- Analyze all folds when making model-level claims.
- Use local artifacts under paths such as:
  - `data_yahoo/tw_stocks/lookback16/explainability`
  - future lookback-32 explainability outputs
- Inspect:
  - feature importance: gradient, integrated gradients, perturbation weight delta
  - time importance by lookback day
  - feature-time heatmaps
  - correlations between raw features and scores/weights
  - stock contribution and concentration
  - aux summaries for latent factors and market tokens
- Be cautious with perturbation `score_abs_delta` when masked scores use sentinel values such as `-1e9`; prefer weight deltas, rank changes, gradients, and integrated gradients.
- Report concentration, turnover, drawdown, and time-attribution issues plainly.

## Testing And Verification

Use focused tests after small changes, then broader tests when training/model/loss code changes.

Common commands:

```bash
/home/user/miniforge3/envs/fintech/bin/python -m py_compile \
  stockagent/config.py \
  stockagent/training/trainer.py \
  stockagent/training/loss.py \
  stockagent/backtest/simulator.py

/home/user/miniforge3/envs/fintech/bin/python -m pytest -q -s test
```

Known repo quirk:

- Running bare `pytest` from repo root may hit an import-file mismatch between root-level `test_mlp_simple.py` and `test/test_mlp_simple.py`.
- Prefer `python -m pytest -q -s test` for the formal test suite unless that quirk is fixed.

Model-specific tests:

```bash
/home/user/miniforge3/envs/fintech/bin/python -m pytest -q -s \
  test/test_low_rank_market_transformer_portfolio.py \
  test/test_explainability_smoke.py
```

Loss/backtest consistency tests:

```bash
/home/user/miniforge3/envs/fintech/bin/python -m pytest -q -s \
  test/test_backtest_tensor_consistency.py \
  test/test_pure_rank_loss.py
```

## Zero-Skill Self-Evolution Protocol

"Zero skill" here means future agents should not rely on hidden memory, private skill files, or unstated assumptions. Improve by observing this repository and recording durable lessons.

Use this loop:

1. Observe: read relevant code, config, logs, curves, metrics, and explainability artifacts.
2. Hypothesize: state the likely bottleneck, bug, or modeling failure in concrete terms.
3. Patch small: make the smallest change that addresses the observed issue.
4. Verify: run py_compile, focused tests, and, when feasible, a short training/explainability smoke run.
5. Record: if the lesson is durable, update `AGENTS.md` or a project note so it does not disappear.

Self-evolution rules:

- Do not add rules based on guesses. Add only rules supported by code, tests, timing data, explainability, or direct user preference.
- Keep enduring policy in `AGENTS.md`; keep long analysis or historical narratives in `docs/`.
- When a rule becomes outdated, revise it instead of accumulating contradictions.
- Prefer measurable criteria:
  - epoch wall time
  - train samples/sec
  - VRAM peak
  - fold-level Sortino/Sharpe/drawdown
  - turnover
  - concentration/HHI/max single-name weight
  - attribution stability across folds
- If a future agent discovers a repeated failure mode, encode the prevention rule here.

## What Not To Do

- Do not silently change return calculation formulas between training and inference.
- Do not optimize only average train step time while ignoring epoch-level overhead.
- Do not skip validation/test/curve/plot/checkpoint timing just to make results look faster.
- Do not introduce full cross-stock attention unless explicitly requested.
- Do not force all financial reductions into BF16.
- Do not move portfolio state to CPU to fix CUDA Graph issues.
- Do not leave config keys that look active but are ignored by factory/model code.
- Do not overwrite user changes in dirty files.

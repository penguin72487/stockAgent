# AGENTS.md

This file records persistent correctness constraints plus the latest measured
engineering recommendations for future coding agents. Read it before changing
code, configs, training logic, model architecture, or explainability artifacts.

Correctness rules (point-in-time data, fee/mask semantics, checkpoint
compatibility, and reproducibility) are contracts. Model choices,
hyperparameters, compile modes, batch sizes, benchmark timings, and the phrase
"active baseline" are experimental snapshots and recommendations, not frozen
requirements. Re-measure them when the hardware, data, objective, or experiment
changes, and follow the user's latest explicit experiment settings.

## Communication

- Reply to the user in Traditional Chinese unless they explicitly request another language.
- Be direct and implementation-oriented. The user usually expects code changes, not only proposals.
- Preserve user intent across turns. In this project, "do not skip" means compute and profile the full epoch workflow unless the user explicitly changes that requirement.
- When explaining trading/model changes, separate engineering facts from investment interpretation.

## Workspace And Environment

- Repo root is the directory containing this file; its absolute path differs across machines.
- Preferred Python runtime is the `fintech` Conda/Mamba environment, whose absolute
  path differs across machines. Source `scripts/runtime_env.sh` and use
  `run_fintech_python`; `FINTECH_ENV_PATH` or `PYTHON_BIN` may override discovery.
- Do not assume `python` exists on PATH or hard-code one user's home directory.
  Run `run_fintech_python scripts/check_environment.py --require-cuda --strict` before expensive jobs.
- CUDA is expected for training. If CUDA is unavailable and `runner.require_cuda` is true, do not silently fall back to CPU.
- Use `rg` / `rg --files` for search.
- Use `apply_patch` for manual file edits.
- Do not revert user changes or unrelated dirty files.
- Do not use destructive git commands such as `git reset --hard` or `git checkout --` unless the user explicitly asks.

## Current Baseline Precision Recommendation

The baseline should use BF16 AMP, not FP16.

Recommended baseline config:

```yaml
environment:
  device: cuda
  use_tensor_cores: true
  amp_dtype: bf16
```

Implementation expectations:

- `stockagent.training.trainer._resolve_amp_dtype("bf16")` must resolve to `torch.bfloat16`.
- Main train/eval/profile model forward and loss computation should run inside `_autocast_context(device, amp_dtype)`.
- BF16 AMP should leave `GradScaler` disabled. `GradScaler` is only for FP16:
  - `GradScaler(enabled=device.type == "cuda" and amp_dtype == torch.float16)`
- Masked score/logit sentinels must be dtype-safe. Do not use fixed `-1e9`
  in model hotpaths, because it overflows FP16 AMP; use a representable finite
  mask fill such as `finite_mask_fill_value(scores)`.
- It is normal and desirable that some tensors remain FP32:
  - model parameters
  - input storage tensors
  - portfolio weights after configured bounded activation + L1 normalization
  - loss/backtest accumulation and numerically sensitive finance metrics
- Do not force the entire pipeline to permanent BF16 storage just to satisfy "BF16"; use AMP for compute and keep sensitive reductions stable.

## Data Panel Backend Contract

The full US Yahoo universe is a distinct data-processing regime. Benchmark it
with the actual full parquet directory instead of small synthetic subsets when
choosing defaults.

Rules:

- `scripts/benchmark_data_backends.py` is the reproducible scanner/benchmark for
  data-processing hotspots. Its active optimization scope is PyArrow plus Polars
  Lazy/Streaming; do not add compatibility/reference paths outside those backends.
- Do not add DuckDB or cuDF back to the panel/runtime benchmark set unless a new
  request explicitly reopens those candidates.
- For full US daily parquet (`16811` files, `S≈16811`) with
  `tradable_mode: tradable`, runtime `build_panel(... panel_backend="auto")`
  should select Polars Lazy when available, then PyArrow. Explicit
  `panel_backend="polars"` is an alias for `polars_lazy`; explicit
  `panel_backend="polars_streaming"` is available for measurement but is not the
  current auto default.
- US Yahoo parquet files under `us_stocks` must keep `Trading_Volume` for
  trainable stock-like assets. If old `_DL`/delisted/archive parquet files are
  missing that column, treat them as schema-broken data: repair should normalize
  `_DL` records back to the base Yahoo symbol and remove unusable delisted
  schema-mismatch files instead of letting panel build fail on the first file.
- For the US Yahoo universe, do not remove a symbol only because it is currently
  delisted; historical delisted common stocks/ADRs/ETFs are needed to reduce
  survivorship bias. Do exclude security types outside the normal broker-tradable
  stock/ETF/ADR universe, such as warrants, rights, units, preferred/depositary
  preferreds, and exchange-listed notes/debt instruments.
- Foreground full-US PyArrow+Polars benchmark after narrowing the scope:
  `panel_build` measured Polars Lazy `69.36s`, Polars Streaming `86.55s`, and
  PyArrow `195.85s` on recheck. PyArrow checksum was
  `8707711790994.017`; Polars Lazy differed by about `913` in feature checksum
  on very large OHLC anomaly-derived ratios, while returns and masks matched in
  spot checks. Use `panel_backend="pyarrow"` when bitwise checksum parity is more
  important than panel-build speed.
- Wide full-US daily weight parquet output (`512 x 16811`) should prefer Polars
  Streaming sink among active native backends: repeat-5 foreground benchmark
  measured Polars Streaming `1.81s`, Polars Lazy `2.13s`, and PyArrow `4.02s`.
- Feature-prep proxy benchmarks favor PyArrow because they use direct Arrow to
  NumPy arrays: PyArrow `29.58s` on recheck, Polars Lazy `206.41s`, and Polars
  Streaming `296.95s`. For the current many-small-files US layout, PyArrow's
  single-pass full-table read is faster than adding a per-file schema projection
  pass.

## Current Main Model Recommendation

The active model is `transformer_base_portfolio`.

The active Transformer-base lookback-32 config is:

```yaml
trading:
  long_only: false
  min_trade_weight: 0.0
  portfolio_activation: identity

training:
  model_name: transformer_base_portfolio
  lookback: 32
  batch_size_train: 32
  batch_size_eval: 16
  enable_torch_compile: true
  auto_torch_compile_sharpe: false
  torch_compile_mode: reduce-overhead
  torchinductor_cache_dir: ~/.cache/torchinductor
  triton_cache_dir: ~/.cache/triton
  cuda_cache_path: ~/.cache/nv_cuda
  compile_loss: true
  loss_portfolio_activation: identity
  auto_batch_size: false
  allow_dynamic_symbols: false
  eval_model_chunk_rows: auto
  eval_backtest_chunk_rows: 512
  eval_backtest_chunk_rows_auto: true
  eval_auto_chunk_rows_cap: 64
  backtest_compile: true
  backtest_compile_stateful: true
  backtest_compile_dynamic: false
  loss_type: log_utility

  transformer_base_portfolio:
    d_model: 32
    attention_mode: market_token
    use_latent_factors: false
    use_market_tokens: true
    use_flash_attention: true
    use_time_pos: true
    use_symbol_pos: true
    input_dropout: 0.0
    sdpa_batch_limit: 16384
    norm_type: rmsnorm
    ffn_type: swiglu
    qk_norm: false
    rope_temporal: true
    rope_base: 10000.0
    temporal_layers: 2
    temporal_heads: 4
    temporal_ffn_mult: 2
    temporal_pooling: attention
    temporal_query_mode: full_then_last
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
    head_hidden_dim: 32
    head_layers: 1
    dropout: 0.1
    default_temperature: 1.0
    portfolio_mode: long_short
    portfolio_output_mode: logits
    max_full_tokens: 16384
    checkpoint_blocks: false
    return_aux: false
    return_aux_details: false
```

Notes:

- The scalable Transformer can be moved from complete to compact via `attention_mode`.
- `transformer_base_portfolio.use_latent_factors` and `use_market_tokens` are
  independent compact-bottleneck switches. `null` preserves the historical
  `attention_mode` preset; explicit booleans override it. The four supported
  combinations within the compact attention family are factor+market,
  factor-only, market-only, and temporal-only.
  Do not enable either compact bottleneck with `attention_mode: full` or `axial`.
- Avoid `attention_mode: full` on a full market universe unless symbol count is small enough for the `max_full_tokens` guard.
- For large universes, prefer `latent`, `latent_only`, or `market_token`.
- `return_aux_details` is useful for explainability but can increase memory pressure during training. Prefer `false` for tight VRAM training and enable it for explainability runs when needed.
- The previous low-rank model remains available as `low_rank_market_transformer_portfolio`.
- Latest active TW full-universe baseline (`S≈2304`) is `attention_mode: market_token`, `lookback: 32`, `batch_size_train: 32`, `batch_size_eval: 16`, and `temporal_pooling: attention`.
- `batch_size_train: 32` improves steady-state epoch throughput versus 16 on the current benchmark, but first-epoch compile/warmup time is higher; use it for long training runs, and re-benchmark before reducing it.
- `temporal_pooling: attention` is the active user preference when trying to improve convergence; pair it with `temporal_query_mode: full_then_last` because attention pooling needs all temporal steps.
- `temporal_pooling: last` remains the faster speed ablation. Pair it with `temporal_query_mode: last_only` to shrink the temporal autograd graph when speed is the priority.
- The active speed ablation sets `qk_norm: false` and `dropout: 0.1` to trim attention/FFN dropout and Q/K RMS-normalization autograd nodes. Treat this as a speed baseline, not proof that the regularized model is worse; re-check validation/test metrics before making investment-quality conclusions.
- `TransformerBasePortfolioModel.forward_from_panel(features, date_indices, mask, ...)` is the preferred lazy-window path for `WindowedSplitTensors`: it projects each unique panel date once and gathers projected `[B,L,S,D]` windows before running the same downstream temporal/mode-specific-bottleneck/score path. Preserve old `forward(x, mask, ...)` API compatibility.
- `TransformerBasePortfolioModel.forward_from_panel_slab(feature_slab, mask, ...)` is the compile-friendly fast path for contiguous lazy-window batches: pass `[B+lookback-1,S,F]` panel slabs and keep `date_indices` / gather metadata outside the compiled model graph. It must remain numerically equivalent to materialized windows and generic `forward_from_panel` for contiguous rows.
- Keep training `return_aux: false` and `return_aux_details: false` unless the objective explicitly needs aux tensors; enable aux for explainability/inference runs rather than the tight VRAM training path.
- The active `market_token` architecture should follow this low-complexity flow:
  - input `[B,L,S,F]` -> feature projection -> shared temporal encoder per stock -> `z_base [B,S,D]`
  - learned static market-token anchors read stocks through cross-attention with stock masks
  - stocks read updated market tokens through cross-attention
  - stock-level market gate applies `z = RMSNorm(z_base_or_factor + sigmoid(g_i) * market_delta)`
  - one configurable scalar `score_head` maps each stock embedding to raw score logits
  - long/short logits are masked and optionally de-meaned; the selected output mode
    either returns logits or transforms them through its configured signed action,
    projection, or activation-plus-L1 rule
- Keep enabled bottleneck tensors available in aux outputs when detailed
  explainability is requested: latent factors for the factor path, market tokens
  plus `stock_market_gate`/`z_market_delta` for the market path, and stock
  embeddings for factor/market bottleneck paths. Do not fabricate disabled-path
  aux tensors.

Modern Transformer module contract:

- Keep residual connections and Pre-Norm.
- Default modern block settings are `norm_type: rmsnorm`, `ffn_type: swiglu`, `qk_norm: true`, `rope_temporal: true`; the current speed ablation deliberately overrides `qk_norm: false`.
- Apply RoPE only to temporal attention by default. Do not apply RoPE over the stock axis unless stock order is deliberately made meaningful.
- Keep PyTorch SDPA/Flash path enabled and keep `sdpa_batch_limit` for large `batch * symbols` temporal attention.
- Transformer-base no longer has dynamic latent/market token delta knobs; use learned static query anchors plus cross-attention. Do not reintroduce no-op config fields for token dynamics.

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
- `latent`: temporal attention, then latent factors and market tokens. This is
  the historical factor+market preset.
- `latent_only`: temporal attention and latent factors without market tokens;
  normally selected with `use_latent_factors: true` and `use_market_tokens: false`.
- `market_token`: temporal attention, then market-token bottleneck. Smaller than latent.
- `temporal_only`: no cross-stock attention. Smallest Transformer baseline.

Rules:

- Keep `use_flash_attention: true` unless debugging. The implementation uses PyTorch SDPA so CUDA can select flash/memory-efficient kernels when shape and dtype allow it.
- Keep `sdpa_batch_limit` enabled for large universes. Temporal attention flattens to `batch_size * symbols`; unchunked SDPA can hit CUDA `invalid argument` when that dimension is too large.
- Do not assume Flash Attention removes full attention compute cost. It reduces memory pressure, but `full` mode is still quadratic in `lookback * stocks`.
- Use `max_full_tokens` as an OOM guard for `full` mode.
- Prefer `latent`, `latent_only`, or `market_token` for full market universes.
- Use `d_model`, layer counts, heads, latent factors, market tokens, and `attention_mode` as the main knobs for scaling small to complete.

## Portfolio Direction Intent

The active baseline should support long/short portfolio weights when the user asks
for multi-directional trading. Earlier experiments used long-only; always follow
the latest explicit user intent and keep model, loss, and backtest settings
aligned.

Guidelines:

- Current active low-rank baseline preference: `portfolio_mode: long_short`.
- Keep `trading.long_only: false` when the model is intended to do long/short.
- Portfolio direction and sizing should default to raw score direction followed by L1 normalization for gross exposure control:
  - `trading.portfolio_activation: identity`
  - `trading.min_trade_weight: 0.0`
  - no activation transform and no minimum-weight threshold suppression unless a config explicitly opts in
  Supported optional activations are `identity`/`none`, `softsign`, `tanh`, `isru`, `erf`, `atan`, and `gd`/`gudermannian`.
- For the active `transformer_base_portfolio` convergence baseline, keep trainable model output decoupled from trading post-processing:
  - `transformer_base_portfolio.portfolio_output_mode: logits`
  - `training.loss_portfolio_activation: identity`
  - `trading.portfolio_activation` remains an optional backtest/inference post-processing knob, so activations such as `gd`, `tanh`, or `isru` can be swept without retraining, but the default is no transform.
- Do not use dual-branch softmax as the active long/short position calculator. Legacy `dual_branch_softmax` / `masked_softmax` names are now compatibility wrappers around configured activation + L1 portfolio normalization.
- If changing `trading.long_only`, understand that it affects loss/backtest interpretation, not just the model head.
- Keep model output mode, loss assumptions, backtest assumptions, and report wording aligned. If they disagree, flag it explicitly.
- `trading.reporting_leverage` is a reporting/post-processing multiplier only.
  Canonical training, validation/test metrics, and integer-share execution keep
  gross exposure at `1.0`; the multiplier produces separate `leverage_*` plots with
  turnover and fees recomputed from scaled weights. It remains part of the
  checkpoint configuration fingerprint so resumed reporting is reproducible.
- Rank-only loss can over-concentrate positions. If using rank objectives, keep turnover/concentration/backtest regularization in mind.
- If the user switches back to only-long behavior, change both the model direction mode and the loss/backtest direction assumptions deliberately and report the change.

## Canonical Tensor Backtest And Loss

The project goal is to keep train, validation, test, and inference return logic consistent and tensor-friendly.

Rules:

- Do not fork separate train/inference return formulas.
- Prefer the canonical tensor backtest in `stockagent/backtest/simulator.py` and loss integration in `stockagent/training/loss.py`.
- Keep computations GPU/tensor-friendly where possible.
- The active loss preference is `log_utility`: maximize annualized mean net log return from canonical `run_backtest_torch` outputs.
- `log_utility` must use fee-adjusted `backtest.strategy_returns`, after `buy_fee_rate` and `sell_fee_rate` have been applied.
- Do not move portfolio state to CPU between batches/chunks.
- Cross-batch/chunk portfolio state should be detached and cloned on GPU:
  - `t.detach().clone(memory_format=torch.contiguous_format)`
- `initial_weights` is trading state, not a gradient path across batches.
- Cross-period state is the previous executed portfolio after mark-to-market
  drift, not the previous target weights. Price moves and paid fees change the
  weights used to compute the next rebalance turnover.
- Carry both `final_weights` and scalar `final_alive` across train/eval chunks.
  Ruin is absorbing; a later model signal must not recreate capital.
- A mandatory short cover floors an executable position at zero but does not
  prohibit a same-day discretionary long. Only the cover-to-zero quantity may
  bypass voluntary turnover/volume limits.
- `CANONICAL_BACKTEST_CONTRACT_VERSION` is part of schema-4 semantic checkpoint
  compatibility. Bump it whenever return, fee, turnover, or recurrent-state
  accounting changes; old-contract weights may be used for inference but must
  not silently resume an optimizer trajectory.
- If compiled loss hits CUDA Graph overwritten-output errors, only fall back the loss wrapper to eager tensor loss; do not disable model `torch.compile` globally.

## Intentional Walk-Forward Semantics

- TW day-trade point-in-time eligibility is absent before 2014 in the verified
  public data and first becomes executable on 2014-01-06 (sell-first coverage
  begins 2014-06-30). A `tw_day_trade` experiment must not start its training
  panel in 2005 or project today's eligibility backward. The trainer's execution-
  coverage preflight must reject any train/validation split with zero executable
  round trips because its canonical loss is constant and all model gradients are
  exactly zero. The active public day-trade config therefore starts in 2014.
- A TW day-trade strategy row executes open[t] to close[t]. Its configured
  symbol benchmark is buy-and-hold over the same wall-clock session, using the
  panel's adjusted-close forward label shifted one row: close[t-1] to close[t].
  Do not replace it with a cross-sectional mean of intraday returns or use the
  unshifted close[t] to close[t+1] label. The active public benchmark is 2330.

- When `walk_forward.require_future_test_year: false`, the final experimental fold
  deliberately reuses its validation window as its test window. Keep that overlap;
  label it as latest-year experimentation rather than unbiased model selection.
- Every split requires a complete lookback contained inside that split. Therefore a
  lookback of 32 deliberately starts evaluation at the split's 32nd trading row
  (drops the first 31 rows). Do not prepend rows from the preceding split to make a
  fold appear to start on its first calendar trading day.
- For stitched deployment tests, the warmup rows before the next model's first valid
  row remain owned by the preceding model. This preserves chronological coverage
  without changing the per-fold lookback rule.

## Trainer Executor Boundaries

- Neural training has one lazy `WindowedSplitTensors` executor per process. The
  single-device and torchrun DDP variants share the same canonical model, loss,
  side masks, fees, and stateful backtest semantics.
- A contiguous fixed-shape batch may use `forward_from_panel_slab`; unsupported,
  non-contiguous, or auxiliary-output cases materialize the window inside that same
  guarded executor. These are input representations, not separate loss/backtest
  implementations.
- LightGBM/XGBoost intentionally retain a separate CPU materialized fit/evaluation
  route because they are a different algorithm family. Do not add another neural
  DataLoader or single-process multi-GPU executor.
- The loss path is canonical `risk_aware_loss` plus `run_backtest_torch`; compile it
  when useful, but do not add an alternate return formula.
- Market configs default to `training.multi_gpu_strategy: auto`: use the
  canonical single-device executor with one visible GPU and automatically
  relaunch torchrun/DDP with two or more visible GPUs. GPU visibility and
  assignment belong to `scripts/manage_gpu_jobs.py`; `tw_parallel` means
  within-fold DDP and should remain semantically aligned with `tw.yaml`.

## Epoch-Level Timing And Throughput

The user cares about total epoch wall time, not only train step time.

Rules:

- Use `epoch_curve.jsonl` when optimizing epoch-level speed.
- Break down "other" time before optimizing blindly.
- For long-year runs, re-check the latest artifact before optimizing. The run under
  `artifacts/train_2000-2001-...-2024/epoch_curve.jsonl` showed train time
  dominating epoch wall time, with CPU-to-GPU train tensor transfer larger than
  model forward. In that case, prioritize guarded GPU train tensor caching over
  test-curve work.
- Every epoch should account for train, validation, sampled test loss, curve test, curve plot, checkpoint, scheduler/progress, and any reporting work.
- Expanding `train_union` folds change the symbol dimension even when the global
  batch dimension is fixed. For compiled canonical loss, mark only the symbol
  axes dynamic, including the direct `symbol_indices [S]` companion and
  recurrent-state vectors, and reuse one compiled loss wrapper across train
  groups. Derive the upper bound from the largest symbol width any current
  walk-forward training group can actually produce after compaction; do not use
  validation/test-only symbols from the full panel. Recompute it every run so
  listings and delistings remain runtime data. An arbitrary very large upper
  bound makes Inductor constraint analysis and cold compilation much more
  expensive and can violate flattened-index guards.
- Do not hide expensive work behind `val_interval_epochs > 1` or skip curve/test/plot work unless the user explicitly asks.
- Recent preference: sampled test loss only needs one fold per epoch to reduce epoch-level overhead.
- Keep curve plotting async where possible.
- When comparing throughput after compile, chunking, or cache changes, use the second epoch or later steady-state numbers. Do not choose defaults from the first epoch, because compile/autotune/warmup can dominate it.
- For the high-throughput TW cash candles configuration, keep
  `finite_check_interval_steps: 0` and `checkpoint_finite_check: false` when the
  user opts out of scanners. Prevent non-finite states in the settlement math
  and bound recurrent differentiation instead of adding parameter/gradient
  sweeps to the training hot path.
- GPU tensor caching is allowed when transfer dominates and VRAM checks pass:
  - prefer `cache_train_tensors_on_gpu: true` for transfer-bound long-year runs
  - keep `cache_eval_tensors_on_gpu: true` for lazy windowed tensor runs when train/val/test can reuse the same cached base panel tensors
  - do not duplicate full `[T,S,F]` panel tensors for train/val/test windowed splits on GPU; cache the base tensors once and share them, moving only split-specific `valid_indices` / `sample_mask`
  - for very large universes such as US full universe (`S≈16808`) on 16GB GPUs, do not force-cache the shared base panel when it would leave too little post-cache VRAM for compiled model/eval workspaces; the safe measured starting point is `batch_size_train: 8`, `batch_size_eval: 8`, `eval_auto_chunk_rows_cap: 16`, `vram_safety_margin_gb: 1.5`, `target_vram_fraction: 0.85`
  - if a windowed shared base remains on CPU, keep windowed metadata on CPU too; CUDA `valid_indices` must not index CPU base tensors
  - prepare lazy train/val/test windowed splits with a shared base so large `[T,S,F]` tensors are not repeatedly pinned or copied before GPU caching; only split metadata should be prepared separately
  - prefer the panel-slab forward wrapper for contiguous train/eval lazy-window batches so `torch.compile` sees fixed slab tensors instead of dynamic date-index gathers; use generic panel forward for non-contiguous rows, factor-augmented/detailed-aux paths, and padded final eval chunks
  - compile the panel-slab forward wrapper with `dynamic=False` and `options={"triton.cudagraphs": False}` rather than `mode="reduce-overhead"`; the reduce-overhead/cudagraphs variant was observed to segfault on the second compiled slab backward for the active RTX 4070 Ti SUPER CUDA environment
  - `_maybe_cache_tensors_on_device` must keep the VRAM safety check and skip caching if it does not fit
  - keep `eval_auto_chunk_rows_cap: 64` for the current speed ablation to reduce validation/test chunk overhead; re-check VRAM and epoch 2+ timing before raising it to 128
  - eval chunk code pads only the final ragged chunk to the configured chunk size and trims outputs back to valid rows; keep this to avoid extra compile shapes without changing canonical returns
  - `eval_auto_chunk_rows_cap: 32` and `batch_size_train: 64` were tested on the current single-fold lookback32 benchmark and were not adopted; cap32 had worse warmup/final eval, batch64 had worse warmup and slower steady epoch

Compile/runtime rules:

- Use CUDA 13 ptxas for the current PyTorch CUDA 13 environment. Prefer mamba/conda packages such as `cuda-nvcc` / `cuda-nvvm-tools` in the `fintech` env; do not leave a CUDA 12 pip `nvidia-cuda-nvcc-cu12` package around as a fallback ptxas source.
- RTX 5070 Ti (`sm_120a`) native NVFP4 uses source-built NVIDIA Transformer Engine, not bitsandbytes NF4 or fake quantization. The verified build is Transformer Engine `2.16.0+4220403`, CUDA 13.3 `nvcc`/`ptxas`, CUDA 13 runtime libraries, and cuDNN 9 in the `fintech` env.
- Build Transformer Engine for Blackwell consumer GPUs with `NVTE_CUDA_ARCHS=120a` / `CMAKE_CUDA_ARCHITECTURES=120a`, and force CUDA library discovery to the conda env. Do not let `libtransformer_engine.so` link to system CUDA 12 libraries; set RPATH or `LD_LIBRARY_PATH` so `libcublas.so.13`, `libcudart.so.13`, `libcublasLt.so.13`, and `libcudnn.so.9` resolve from the `fintech` env.
- On RTX 5070 Ti, NVFP4 stochastic rounding is not supported by ptxas for `sm_120a` (`cvt.rs.satfinite.e2m1x4.f32` rejects `.rs`). Use native deterministic NVFP4 recipes such as `NVFP4BlockScaling(disable_rht=True, disable_stochastic_rounding=True)` for project benchmarks and training probes.
- Transformer Engine NVFP4 `te.Linear` needs a conservative padding adapter in this project: pad input/K to 32, output/N to 32, and leading rows to 32, then slice outputs back. The lower FP4 type granularity is 16, but `K=48` FFN output projections failed on RTX 5070 Ti unless padded to `K=64`.
- Source `scripts/runtime_env.sh` before invoking Python. It prepends the selected
  environment's `bin`, so Triton resolves that environment's `ptxas` regardless
  of where the environment is installed.
- Compile cache paths should be stable and persistent across runs:
  - `TORCHINDUCTOR_CACHE_DIR=~/.cache/torchinductor`
  - `TRITON_CACHE_DIR=~/.cache/triton`
  - `CUDA_CACHE_PATH=~/.cache/nv_cuda`
  - do not delete these caches between repeated same-shape benchmarks unless explicitly testing cold compile behavior
- On the measured dual-RTX-5090 TW public run, letting each DDP rank inherit 64
  Inductor compile workers created 128 concurrent workers. After an interrupted
  compile they became orphaned and remained blocked in XFS
  `filename_create`/`xfs_buf_lock`, stalling later pre-epoch work. Keep the
  `tw_public` host-wide `environment.torch_compile_threads` budget at 16 (8 per
  rank), serialize the independent DDP model probe so the later rank reuses the
  persistent cache, and preserve graceful SIGTERM-to-atexit cleanup for
  Inductor workers. The canonical-loss probe is intentionally collective: it
  must reproduce the real autograd all-gather input on all ranks, otherwise the
  first train step compiles the same loss again.
- After DDP training, run the saved-model inference/plot artifact pass on rank
  0 only. Other ranks must wait through a dedicated CPU/Gloo process group with
  a long artifact-I/O timeout; do not use the default NCCL group for this wait.
  A measured XFS discard stall exceeded NCCL's 600-second watchdog even though
  no GPU work was wrong, while the Gloo wait completed normally and avoided a
  duplicate inference pass and artifact write race.
- The TW public open-aware `tw_day_trade` executor now uses the same bounded
  compiled-settlement pattern as `tw_cash`. Keep the eager ledger as the
  semantic oracle, compile fixed 32-row chunks with CUDA graphs disabled, and
  carry `cash`, `payables`, `receivables`, `alive`, and `equity_scale`
  differentiably between chunks. A non-aligned tail stays on the exact eager
  implementation. Do not replace this state machine with independent daily
  returns: T+2 default is absorbing and volume caps depend on carried equity.
  The DDP canonical-loss probe must report `tw_day_trade_chunked=true`, at
  least one compiled chunk call, and zero eager fallbacks before epoch 1.
- Measured on dual RTX 5090 with the open-aware public config, `T=128`,
  `S=2738`, and chunk/horizon 32: settlement forward+backward improved from
  `546.7ms` eager to `114.1ms` compiled in an isolated actual-shape probe; a
  no-grad `T=512` evaluation improved from `950.0ms` to `68.5ms`. The complete
  fold-1 epoch-2/3 median improved from the prior `2.213s` baseline to
  `0.439s`, including validation, sampled test curve, plotting, and checkpoint
  work. Cold compile remains material (roughly 100 seconds per grad/no-grad
  contract), so preserve stable caches and judge throughput only after epoch 1.
- Expanding `train_union` folds change their symbol count. When
  `training.compile_loss_dynamic_symbols: true`, keep the time/batch axis static,
  mark only the canonical loss symbol axis dynamic, and reuse one compiled loss
  wrapper across train groups. This is an executor optimization: do not pad real
  assets, fork the loss formula, or add it to the semantic checkpoint contract.
- Current benchmark result for the active `data_okx` lookback32 run: compare only epoch 2 or later. The fastest measured compile combination was model compile plus the canonical fullgraph log-utility loss:
  - `enable_torch_compile: true`
  - `backtest_compile: true`
  - `backtest_compile_stateful: true`
  - `backtest_compile_dynamic: false` for fixed train/eval shapes
  - `compile_loss: true`
  - epoch 2 wall time improved from about `67.54s` with all compile off to about `18.99s`.
- Compile mode benchmark result:
  - keep `torch_compile_mode: reduce-overhead`
  - `default` and `max-autotune` were slower on epoch 2 for the active `data_okx` lookback32 shape
- Current chunk/batch benchmark result:
  - keep `eval_model_chunk_rows: auto` with `eval_auto_chunk_rows_cap: 64`
  - keep `eval_backtest_chunk_rows: 512`; larger compiled backtest chunks such as 1024/2048 stalled compilation and did not produce epoch 2 within the manual test window
  - keep `batch_size_train: 32`; `batch_size_train: 64` was only marginally faster in one epoch-2 run and changes optimizer batch granularity
  - keep `backtest_autotune: true`; disabling it was only noise-level faster in one epoch-2 run and can hurt other shapes
  - keep backtest prep compile enabled; `STOCKAGENT_BACKTEST_COMPILE_PREP=0` was not faster on epoch 2
- Trainer compile checks should discover the selected fintech environment's
  `ptxas` and conda compilers `x86_64-conda-linux-gnu-gcc/g++` even when the
  parent shell PATH is sparse.
- Historical actual-shape compile probes on the 2000-2024 TW checkpoint showed:
  - compiled `transformer_base_portfolio` model forward is beneficial
  - compiled tensor backtest is beneficial and may use fallback on unsupported graph states
  - isolated compiled loss has small benefit, but compiled model plus compiled loss was unstable in the actual-shape probe
- Current safe baseline preference:
  - `enable_torch_compile: true`
  - `auto_torch_compile_sharpe: false`
  - `backtest_compile: true`
  - `backtest_compile_stateful: true`
  - `backtest_compile_dynamic: false`
  - `backtest_autotune: true`
  - `compile_loss: true`
  - compile canonical `risk_aware_loss` with `fullgraph=true` for log utility; do not maintain a second loss formula
- Eval model forward chunking and eval backtest chunking are intentionally decoupled:
  - keep model chunk sizing VRAM-driven, often `eval_model_chunk_rows: auto`
  - use larger `eval_backtest_chunk_rows`, currently `512`, to reduce `run_backtest_torch()` calls without skipping any val/test curve rows
  - preserve `prev_weights` continuation across backtest chunks and reset only at fold/segment boundaries

## Crypto Downloader Baseline

The active crypto downloader baseline is 15-minute bars.

Rules:

- Yahoo `crypto`, OKX perpetual, and Bybit perpetual downloaders should treat 15m candles as the source of truth.
- Do not silently merge old daily crypto parquet rows with new 15m rows in the same file.
- If an existing crypto parquet file looks like a daily-frequency artifact, rebuild it from the 15m source instead of appending to it.
- Keep stock and FX Yahoo downloads on daily bars unless the user explicitly changes those markets too.

## Feature Engineering Guardrails

Explainability indicated suspicious dependence on raw price level and raw liquidity.

Rules:

- Avoid feeding raw OHLC price levels directly when the goal is cross-stock generalization.
- Prefer log returns, relative price ratios, rolling normalization, and engineered K-line/volume features.
- If changing feature schema, update cache/versioning so stale panel caches are not reused.
- Keep `return_1d`, tradable masks, TW limit guards, and benchmark construction aligned with the canonical backtest.
- TW public snapshot-only families are permanently forbidden model inputs. Never
  add them to `data.feature_include`, model categorical-feature lists,
  explainability selection, or a replacement model schema: `twpub_monthly_revenue_*`,
  `twpub_cumulative_revenue_yoy`, `twpub_financial_*`, `twpub_insider_*`,
  `twpub_borrow_*`, `twpub_sbl_*`, `twpub_short_sale_available_*`,
  `twpub_tdcc_*`, and `twpub_company_*`. Their raw/source columns may remain
  available solely for provenance, auditing, and future data-quality research;
  they must not influence training, validation, test, inference, or feature
  importance.

### TW Day-Trade Open-Aware Feature Contract

- The only model-visible session-`t` opening quote is the opt-in normalized
  feature `next_session_open_gap_logret`. Panel row `r` stores
  `log(open[r+1] / close[r])`; because `tw_day_trade` keeps its ordinary
  execution feature lag at one, the final row of a target-`t` window exposes
  exactly `log(open[t] / close[t-1])`.
- Do not replace this with a raw nominal opening-price feature. The normalized
  gap is invariant to stock price scale and does not let the model identify a
  symbol merely from its price level.
- This feature is opt-in and legal only with `trading.execution_mode:
  tw_day_trade`. Empty/wildcard panel feature selection must retain the
  close-complete schema and must not silently include a next-session quote.
- No session-`t` high, low, close, or volume may enter the model window used to
  submit its opening order. Same-session full-day volume is future information.
  Execution participation limits must continue to use completed session `t-1`
  share volume as their causal proxy and keep that volume in the execution
  layer, not in the model input.
- Historical 98-feature close-complete checkpoints remain a separate schema.
  Use a new artifact root and retrain for the 99-feature open-aware model.

## TW Public Execution-Rule Contract

- Never put a data-dependent `torch._assert_async` in a compiled model,
  settlement, loss, or backtest CUDA hot path. A failed device assertion poisons
  the CUDA context and causes every DDP/NCCL rank to abort. Validate static input
  contracts at eager host boundaries. For runtime market facts, use deterministic
  tensor semantics: an impossible mandatory exit is an absorbing account default;
  a day-trade round trip with a missing close leg or invalid valuation never opens.
- `tw_cash.short_maintenance_ratio` limits collateral released by a cover. If an
  account was already below maintenance before a partial cover, release zero
  collateral and retain/reassign the existing pools; do not assert and do not
  invent an unconfigured same-day margin-call cure. A complete cover still
  releases all collateral. This v8 return/default change is part of
  `CANONICAL_BACKTEST_CONTRACT_VERSION`.
- Corporate-action receipt `requested_start_year` is the latest incremental
  downloader request boundary, not the historical archive boundary. Panel
  completeness and TW cash avoidance must use cumulative
  `coverage_start_year` (falling back only for legacy receipts), and that
  interpretation must remain part of the panel cache contract key.
- Preserve two distinct receipt-derived corporate-action masks. Avoid mode uses
  the full interval from the last close where both long and short positions can
  be flattened through the last cum-right close for every official action.
  Exact mode uses the unresolved-only interval plus the issuer entitlement and
  payment ledger. Never reconstruct avoid mode by OR-ing a one-row cash-yield
  event into the unresolved mask; the event row can be halted or limit-blocked.
  The effective mode-specific mask is part of the checkpoint data fingerprint.

- Use `downloader/download_tw_official_data.py` as the canonical TWSE/TPEx-first
  data-layer entry point. Its modes are `rebuild` (staged from-zero replacement),
  `repair` (audit local historical coverage and fetch missing/suspicious dates),
  and `daily` (verified-baseline incremental update with a recent correction
  overlap). Daily mode must fail when no completed rebuild/repair baseline exists.
- Canonical TW OHLCV starts at 2000-01-01. TWSE/TPEx rows always win the same
  `date + symbol` key. The approved `yahoo_fallback` may fill only otherwise
  missing stock/ETF OHLCV rows from 2000 onward; it must pass through
  `scripts/build_tw_yahoo_fallback_archive.py`, preserve row-level `data_source`,
  preserve `adjustment_source` when Yahoo supplies only a missing return factor,
  and retain content receipts. Official OHLCV must remain untouched when only its
  reference/change factor is missing. Yahoo must not fill public features, execution
  rules, valuation, margin, institutional, lifecycle, or corporate-action data.
- Source retention and the model horizon are separate contracts. The current
  receipt-certified archive keeps 2000+ rows, but 80 official delisted-company
  histories ending no later than 2004-04-28 are terminal-unavailable from Yahoo
  and precede complete free official coverage. Do not fabricate or silently
  ignore them. Until a provenance-backed backfill is obtained, TW training
  configs must use `data.panel_start_date: 2005-01-01`, which yields the first
  session 2005-01-03 and 100% official-delisted coverage inside the model
  horizon. Audit retained source rows against the full verified TAIEX calendar
  while auditing panel/universe/walk-forward semantics against the clipped model
  calendar. A newly proven backfill may reopen the earlier horizon only after a
  strict re-audit.
- Changing `data.panel_start_date` changes the experiment and checkpoint
  identity. With a 2005 first year, validation years 2023/2024/2025 are folds
  18/19/20, not 23/24/25. Keep `walk_forward.expected_first_year`, runner fold
  selection, explainability, benchmarks, and cached panel keys aligned.
- Fresh TW Yahoo fallback downloads default to one worker and a 1.5-second global
  request interval. If the bare chart endpoint is rate-limited, the installed
  yfinance session is the same-provider fallback. Persist
  `stockagent.yahoo_requested_start`; repair must re-query from 2000 when that
  historical coverage receipt is absent. Reusing one fixed rebuild stage must
  skip already-completed atomic symbol files.
- Direct Yahoo downloader invocations use the same provider-named host-global
  limiter and default to 10 requests/second when `--request-interval` is omitted;
  the staged TW bootstrap deliberately overrides that policy with the slower
  1.5-second interval above. A repair symbol universe must be the stable union of
  live discovery, cached/repository manifests, locally tracked parquet, and the
  canonical TWSE/TPEx delisted-company parquets. Do not let a successful current
  listing query erase historical or delisted symbols.
- A Yahoo TW source parquet is eligible for the lower-priority archive only when
  its schema metadata says `stockagent.source=yahoo`,
  `stockagent.asset_class=tw_stocks`, its `yahoo_requested_start` reaches the
  requested archive start, and `yahoo_checked_through` reaches the requested end.
  The archive must account for every manifest symbol as either a verified source
  file or a terminal unavailable result, write full per-file size/SHA-256 and
  coverage metadata to the adjacent `.inputs.json`, and receipt that manifest in
  the summary. Downstream symbol build/audit must fail closed on a missing,
  stale, or tampered input receipt chain.
- When Yahoo fallback is enabled, build the receipt-verified
  `tw_transfer_adjustment_reference.parquet` stage after the per-symbol Yahoo
  source update and before `build_tw_yahoo_fallback_archive.py`. Its official
  requests use the `tw_public` provider-global limiter; an omitted interval keeps
  the project default of 10 requests/second. The archive may fill
  `source_factor` only when a reference `date + symbol` matches that canonical
  Yahoo source's first retained row and the original factor is null. Every
  reference key must be applied exactly once; incomplete coverage, unresolved
  rows, stale input/output receipts, non-first-row matches, duplicate matches,
  unmatched keys, or an empty historical candidate set fail closed. Receipt
  both the reference parquet and its summary in the Yahoo archive input manifest
  and reconcile required-candidate, reference, and applied counts. Do not run
  this stage with `--ohlcv-fallback none`.
- Historical downloader success is coverage-based, not inferred from receiving
  any rows. Persist confirmed no-data weekdays separately from request failures;
  any unresolved date failure must produce a nonzero exit. A failed rebuild must
  leave production parquet files untouched.
- Historical rebuild request outcomes are append-only per-date JSONL journals
  under `state/journals`, with successful parsed rows retained in fingerprinted
  `state/partials` parquet while coverage is incomplete. Default `--resume` must
  reuse validated partial dates, confirmed-empty journal events, and reparsable
  atomic raw receipts, then request only unresolved/failed/corrupt/suspicious
  dates. Partial success remains nonzero and must never weaken the final
  coverage/audit/promotion gate. `--no-resume` is the explicit fresh-refetch
  escape hatch. Increment `HISTORICAL_PARSER_CONTRACT_VERSION` whenever raw
  historical response parsing semantics change so an old parsed partial is not
  silently reused under a new parser. A parser bump may reparse a prior
  content-addressed `raw_failures` receipt across old cache keys only when its
  append-only event is `failed/network/HTTP 200`, its official URL and date
  match, its path stays under the dataset failure directory, and filename,
  byte counts, full raw/body SHA-256, nonempty current parse, and current schema
  all validate. Any mismatch remains unresolved for a network retry. On POSIX,
  each historical dataset also
  holds a nonblocking process lock at `state/locks/<dataset>.lock` for the whole
  mutation; a second writer to the same dataset/stage must fail immediately.
- Canonical historical runs must use the receipt-verified monthly TAIEX archive
  as their actual-session calendar. Keep `twse_daily_ohlcv` and
  `twse_market_index` at parser contract v7. A measured rebuild disproved the
  old assumption that a selected MI_INDEX table title was authoritative: TWSE
  returned bodies whose table title was rewritten to the requested day while
  top-level `payload.date`, `params.date`, and all data rows belonged to another
  session. This
  affected 18 real-session OHLCV receipts and 7 TAIEX closes, not only holidays.
  v7 requires the selected table title, top-level `payload.date`, and any
  supplied `params.date` to declare the requested date. Never bind a retitled
  body to `request_date`, and
  never reuse v6 partials as current parsed data. During a resumed rebuild,
  reparse raw receipts into the v7 partial, reuse only receipts that pass every
  date check, and network-refetch the rejected dates; stale receipt bytes
  cannot be repaired by reparsing. For legacy TPEx rows with the exact official
  sentinel
  `open=high=low=0` and a positive close, preserve the raw row; the derived
  symbol parquet may create a flat bar at that official close only when it also
  records `ohlc_normalization=official_close_flat_bar`. Yahoo still must not
  overwrite that official row.
- Canonical Yahoo OHLCV fallback must be bounded by receipt-verified official
  security lifecycle episodes. After a terminal TWSE/TPEx delisting, discard
  fallback rows until a later current-company, new-listing, or official daily
  listing observation verifies a new episode; reset the derived adjustment
  index to 10 at that episode boundary. A same-day or next-official-session
  same-symbol venue migration is a nonterminal continuation, including official
  `櫃轉市`, and must not be truncated or reset. Persist lifecycle evidence on
  retained rows and reconcile every lifecycle-filtered fallback row in the
  symbol-build summary. Never attach a later Yahoo reuse of a code (for example
  post-2007 `9801`) to the old delisted company without official relisting
  evidence.
- A TPEx row's `次日參考價` prices only the immediately following
  receipt-verified official session. When today's exact adjustment reference is
  otherwise unavailable, the builder may use
  `close_today / previous_session.next_reference` only when the previous symbol
  row is that exact preceding session, both rows remain in the same lifecycle
  episode, and the reference and close are positive finite values. Record the
  previous session/date/reference as row provenance and reconcile the candidate
  and applied counts in the build summary. Never use today's `次日參考價` as
  today's reference, and never bridge a missing session or lifecycle boundary.
- Keep the TPEx daily parser at contract v12 for the verified layout sequence:
  width 27 on 2003-08-01--2004-01-30, width 26 on
  2004-02-02--2004-10-27, width 18 on 2004-10-28--2004-11-24,
  width 19 on 2004-11-25--2006-12-29, and width 17 in the legacy JSON `html`
  on 2007-01-02--2007-06-29. Preserve every available quote/statistics field.
  Bind archive dates only from a labeled compact ROC date or the exact damaged
  Oracle header cell (`width=71`, `colspan=5`, `rowspan=2`,
  `class=table-body-right`, one `<tt>` date), and require it to match the request.
- TPEx v12 may preserve permanently damaged security names only with
  `_name_decode_status=official_receipt_name_bytes_unrecoverable`. Receipt-level
  replacement-byte evidence applies that status to every name row in the
  receipt, because CP950 can re-pair `EF BF BD` into plausible CJK text. It may recover
  only the three evidenced CP950 change renderings for `除權`, `除息`, and
  `除權息`, recording `_change_decode_status`; any unknown damaged symbol,
  numeric, or change token fails closed. A `均價=註` row is valid only under the
  exact zero-price/zero-change/zero-volume/zero-amount/zero-trades gate.
- A TPEx row with all four prices zero but positive, internally consistent
  volume/amount/average is an official unpriceable observation, not a usable
  OHLC bar. Preserve it in raw public data; never substitute average for close.
  A valid Yahoo bar may fill the canonical key with
  `fallback_reason=official_ohlcv_unusable`. The symbol-build summary must
  reconcile `official_unusable_ohlcv_rows` as
  `fallback_replaced_unusable_official_rows + unfilled_unusable_official_rows`.
- Keep `tpex_margin_balance` at parser contract v8. v8 adds the exact
  2004-10-19 onward 16-cell generation, preserving the real trailing blank
  note cell, plus its narrowly styled standalone ROC-date header. Do not accept
  a 15-cell row: preserving the blank `<td>` is what distinguishes an empty
  note from a genuinely missing column. The v7 source-gap contract remains:
  the official backend has a verified 331-open-session archive gap from
  2007-06-01 through 2008-09-29
  (data exists through 2007-05-31 and again from 2008-09-30). Never infer the
  whole gap from its bounds: each session counts for coverage/resume only after
  an HTTP 200 explicit-no-data body is saved immutably under
  `raw_empty/tpex_margin_balance`, and its journal URL, byte counts, and body/raw
  SHA-256 all verify. This receipt is mandatory even with `--skip-raw`; an empty
  outside the declared range or any receipt mismatch remains a failure, while
  nonempty official data inside the range remains data.
- Daily margin/short balances and TWSE/TPEx institutional flows are post-close
  histories, not intraday information. Their source row belongs to session `t`,
  but model features become available only on the next receipt-verified TAIEX
  session. Preserve margin short-capacity rule evidence on source session `t`
  because the panel already applies that explicitly next-session rule once;
  never double-shift it. Current-only SBL/borrow/available-short OpenAPI rows
  remain snapshot features and must stay excluded until a historical archive is
  obtained.
- The current `tw_public` `naive` research baseline deliberately approximates
  execution at the final regular close while allowing final session-t OHLCV,
  candlestick geometry, and official daily liquidity aggregates in that same
  signal. This is not a realizable closing-auction timing contract. Keep
  `data.allow_same_close_feature_approximation: true` explicit, keep these 22
  fields out of `data.feature_shift_next_session`, and caveat resulting metrics.
  A future execution-aware implementation should replace this approximation
  with an order cutoff plus an executable price model. This exception does not
  apply to post-close margin/short/institutional histories: they remain shifted
  to the next verified session. The optional panel-shift mechanism remains part
  of the preprocessing/checkpoint fingerprint for configurations that use it,
  and must happen before `panel_start_date` slicing.
- Keep `tpex_daily_valuation` at parser contract v7. The 2004--2006 archive
  declares its requested day as a labeled ROC date such as
  `交易日期:94年08月08日`; bind that exact date and still fail on missing or
  mismatched labels.
- Public HTTP throttling is provider-named and host-global across stockAgent
  threads and subprocesses. For an upstream without a documented numeric limit,
  the project default is 10 requests/second; this is a client-side safety policy,
  not an official allowance. Explicit slower intervals are valid, and 403/429,
  `Retry-After`, or transient-server backoff must defer the shared provider
  schedule. Treat TWSE's HTTP 307 `FOR SECURITY REASONS` page as a provider-wide
  WAF signal. For `twse_market_index`, cross-check primary `rwd` failures and
  structured weekday empties through the official
  `exchangeReport/MI_INDEX?type=IND` route; reliable `IND` coverage starts on
  2009-01-05. The four other TWSE histories use official legacy fallbacks:
  `MI_INDEX?type=ALLBUT0999` for daily OHLCV, `exchangeReport/BWIBBU_d` for
  valuation, `fund/T86` for institutions, and `exchangeReport/MI_MARGN` for
  margin. Retry a semantically stale fallback with a unique `_` cachebuster.
  Under the v7 MI_INDEX contract, validate the selected target-table title,
  top-level `payload.date`, and any supplied `params.date` together; a retitled
  table never overrides a stale top-level or parameter date.
  A live WAF recovery recheck used the provider-global
  `--request-interval 1.0` (one request/second); retain that slower measured
  setting for WAF-sensitive repair rather than treating 10 req/s as guaranteed.
  The official findings and URLs live in
  `docs/tw_public_download_resume_and_rate_limits.md`.
- For an HTTP 200 body that fails semantic parsing, discard only the current
  thread-local HTTP session before route/retry/cachebuster handling. Do not add
  another provider-global defer or sleep for that semantic retry: its next HTTP
  call still passes through the 10 req/s default limiter, while WAF, 429,
  transport, and `Retry-After` backoff remain provider-global inside `_http_get`.
- Strict-calendar state finalization must prune malformed or stale non-session
  `failed_dates` keys, plus failures resolved by verified data/empty receipts.
  Keep actual unresolved session failures. Record the last prune count/examples
  and cumulative `pruned_failed_dates_total` so cleanup can make coverage
  complete without erasing the audit trail.
- Canonical TW stock/ETF symbol files live under `data_tw_public/stocks`. Do not
  run the former in-place official-to-Yahoo mutation script; the canonical
  lower-priority archive merge is the only approved Yahoo fallback path.

- Backfill official lifecycle/short-sale announcements with
  `run_fintech_python downloader/download_tw_short_sale_restrictions.py --output-dir data_tw_public --start-year 1995 --end-year <year>`, then rebuild
  `data_tw_public/features/tw_public_stock_daily.parquet` with
  `scripts/build_tw_public_training_features.py`. The downloader is strict by
  default: it writes a completeness report and refuses to replace data after an
  incomplete archive request unless `--allow-partial` is explicitly chosen.

- `data.use_tw_public_features` controls model inputs; `data.use_tw_public_rules`
  independently controls execution masks. A rules-only TW baseline must not append
  `twpub_*` features or read their parquet columns.
- When `use_tw_public_rules: true`, the configured public parquet is required;
  fail before panel construction instead of silently training without market rules.
- `can_sell_mask` means an existing long may be sold. `can_short_open_mask` means a
  new/increased short may be opened. Do not merge them: a short-sale ban must not
  prevent an investor from selling an owned long position.
- Ordinary non-tradability, missing data, zero volume, halts, and price-limit blocks
  freeze the affected position. Only an explicit official permanent-exit event sets
  `force_exit_mask` and settles a position (with the applicable buy/sell fee).
- An official permanent-exit date may follow a quote-less suspension. Place its
  `force_exit_mask` on the final finite positive close of that security episode,
  while blocking the delisted interval from the official event date onward. Never
  ask exact-share execution to fabricate a termination-day price or reuse a prior
  incarnation's close after a genuine relisting boundary.
- Exact-share holdings reports must reconcile claims, risky marks, collateral,
  and NAV at one accounting instant. A cash-dividend receivable earned after the
  row's execution mark belongs to end-of-row queues, not the execution-time cash
  row; preserve separate execution-time queue totals instead of mixing them.
- Do not treat every venue-level delisting as a terminal asset exit. TPEx rows
  identified by the official TWSE new-listing feed as `櫃轉市` continue under the
  same symbol and must not fabricate a sale/fee. Likewise, if a symbol resumes on
  the immediately following panel session, treat it as a same-symbol market or
  corporate transition; a later relisting after a real gap remains a new
  incarnation and the old position exits first.
- `_twpub_official_traded` may contain disjoint archive snapshots. Infer a missing
  trading session only between nearby observations (currently at most seven
  calendar days); never convert a long source-coverage gap into a multi-year halt.
  Long halts come from explicit official halt/resume events.
- Fund notices often write an ETF code only in parentheses. Keep broker-tradable
  ETF beneficiary certificates in the short-ban/terminal parser while continuing
  to exclude warrants, ETNs, preferreds, and ordinary debt instruments.
- Delisting and short-cover announcements are point-in-time state transitions.
  Process them chronologically by market and symbol, and allow a later cancellation
  to remove only a still-pending delisting/cover obligation while preserving an
  explicitly continuing short-open ban.
- Relative cover rules use their stated anchor. The usual rule is ten exchange
  sessions before delisting; notices that say six sessions before stop-transfer use
  the stop-transfer start date instead.
- Panel cache v2 writes immutable generations under a writer lock and atomically
  commits metadata last. Readers retry the complete snapshot if a concurrent writer
  reclaims the generation sampled by an earlier metadata read.
- Panel cache validation fingerprints every source byte (including the external
  rule parquet), so a same-size replacement with preserved timestamps cannot
  silently reuse stale execution masks.

## Explainability Contract

The user wants detailed model explainability to detect strange rules and judge strategy trustworthiness.

Expected explainability workflow:

- `run_fintech_python explain_model.py` should default to drawing the full explainability set unless the user asks for a smaller run.
- Do not generate Top-K/Top-N explainability tables or charts. Default portfolio
  attribution must cover every tradable non-zero position and report both
  position-count and gross-exposure coverage. Persist the complete sampled
  date-symbol inventory, including flat and masked rows, so omissions are auditable.
  Present high-dimensional results as complete matrices, distributions, cumulative
  coverage curves, or full machine-readable tables rather than rank truncation.
- Persist an explainability completeness table that reconciles decision-inventory
  rows, attributed positions, gross exposure, and enabled lookback-by-feature
  cells. Distinguish completeness within sampled dates from date coverage; an
  explicit exhaustive-date run must remain available without changing model semantics.
- Analyze all folds when making model-level claims.
- Keep `training.explain_after_each_fold: false` by default so training VRAM/time stays focused on train/eval/test artifacts.
- Generate explainability after training with `run_fintech_python explain_model.py`, which defaults to scanning all folds that have `checkpoint_best.pt`.
- Only enable `training.explain_after_each_fold: true` for deliberate smoke/debug runs, because paper explainability can be slow and VRAM-heavy.
- All model explainability and feature-screening calculations have one fixed
  comparison window: the first calendar year of each fold's test split, using
  every valid date after the in-split lookback. This is a correctness contract,
  not a default. Explainability has no train/validation split selector and no
  alternate test-year coverage selector. Do not reintroduce config fields, CLI
  flags, aliases, or helper branches that allow train/validation rows, later
  test years, or the complete future test tail into an explainability
  calculation. Positive within-year date limits remain explicit smoke/debug
  reductions only. Analyze all folds before making model-level or feature-
  selection claims so every fold contributes one comparable test year.
- Feature-selection screening must require every configured fold checkpoint and
  process every valid date in the fixed first-test-year window. Do not add fold
  subset or date-sampling CLI options to the screening runner; only reuse saved
  attribution files after validating identical complete coverage.
- The active explainability feature-selection rule keeps a feature if either
  Gradient x Input or Integrated Gradients is non-zero in at least one fold.
  Disable/comment a feature only when both methods are exactly zero in every
  configured fold; do not apply a small-contribution threshold unless the user
  explicitly changes this rule.
- Standalone `explain_model.py` must enable gradient × input, Integrated
  Gradients, feature-time perturbation, surrogate SHAP, regime analysis, fold
  stability, aux diagnostics, and all eligible cuML UMAP projections. Cross-asset
  GPU work is an independently scheduled project: run `cross_asset_model.py`,
  which owns cross-asset shocks, attention flow, validated transmission, role
  embeddings, and graph explainability. Its default artifacts live under the
  explicitly selected training root at `explainability/fold_XX_test/`, alongside
  the other fold explainability products. Do not add a compatibility cross-asset switch
  back to `explain_model.py` or the training CLI; the standalone runner is the
  only execution entry point.
  Chunking may change execution shape but must not omit outputs within the
  selected project.
- `cross_asset_model.py` has a fixed coverage contract: all configured folds,
  canonical `checkpoint_best.pt`, the first calendar year of each test split,
  and every valid date after the in-split lookback. Do not reintroduce
  fold/checkpoint/split/date-sampling/all-test-years CLI paths. Torchrun ranks
  independently own whole folds; do not wrap the model in gradient DDP.
- On the measured dual-RTX-5090 TW public shape (`L=32`, `S=2735`, `F=131`),
  use BF16, temporal-stock embedding reuse, compiled post-temporal forward,
  `source_chunk_size: 128`, automatic `row_chunk_size: 32`, and
  `max_repeated_rows: 4096`. The 4K scenario batch measured about 45.4k
  scenarios/s/GPU before source-score scatter vectorization and about 47.2k
  afterward, with 96.8% average SM utilization and roughly 99% of the measured
  required-kernel roof. An 8K batch reserved about 28.9GiB without material
  throughput gain and 16K OOMed; re-profile before changing the 4K default.
- Accumulate cross-asset metrics on GPU and transfer once per shock. Production
  output stores the complete numeric metrics once in compact edge Parquet plus
  source/target/shock lookup tables; do not duplicate the same full-universe
  values into dozens of dense matrix files.
- Cross-asset graph figures must not use Top-K/Top-N node or edge selection.
  Render every inter-symbol edge in the directed topology adjacency map and
  every graph node in importance/self-influence figures. Sparse tick labels are
  a layout choice only and must not omit matrix cells, graph metrics, or
  betweenness computation.
- Long standalone explainability stages should expose tqdm progress with ETA and
  throughput. Persist per-stage compute/write/cross-asset timings plus CUDA peak
  memory in the fold explainability timing artifacts; `--no-progress` may hide
  terminal bars but must not disable timing collection.
- Paper-grade explainability is the default report style:
  - `explain_report_style: paper`
  - `explain_plot_theme: paper`
  - `explain_shap_enabled: true`
  - `explain_shap_mode: score_head_surrogate`
- Use local artifacts under paths such as:
  - `data_yahoo/tw_stocks/lookback16/explainability`
  - future lookback-32 explainability outputs
- Inspect:
  - feature importance: gradient, integrated gradients, perturbation weight delta
  - time importance by lookback day
  - feature-time heatmaps
  - correlations between raw features and scores/weights
  - stock contribution and concentration
  - aux summaries for enabled latent factors and/or market tokens
- Dense explainability plots should use RAPIDS/cuDF/Datashader when available.
- Dimensionality reduction for transformer aux tensors should use cuML UMAP, not PCA, for the default explainability projection path.
- Aux UMAP projection outputs live under `aux_projections/*.csv` and `plots/aux_umap/*.png`; use them to inspect stock embeddings, enabled latent factors/market tokens, and token collapse/regime clustering.
- Be cautious with perturbation `score_abs_delta` when masked scores use sentinel values such as `-1e9`; prefer weight deltas, rank changes, gradients, and integrated gradients.
- Report concentration, turnover, drawdown, and time-attribution issues plainly.
- Paper outputs should be generated under:
  - `plots_paper/*.png`
  - `paper_tables/*.csv`
  - `paper_explainability_report.md`
  - `paper_explainability_summary.json`
- If `config_lookback` and attribution lookback differ, the paper report must warn that the artifact is not a complete explanation for that lookback.

Plot/backend rules:

- PyQtGraph is for live scalar monitoring from streams such as `epoch_curve.jsonl`; do not put a GUI event loop in the trainer main path.
- Plotly is for optional interactive dashboards from saved CSV artifacts; do not make Plotly a required training dependency.
- SHAP for `transformer_base_portfolio` should use score-head/surrogate SHAP by default. Do not run full `[batch, lookback, symbols, features]` tensor SHAP except as a tiny explicit case study.
- Datashader is the preferred backend for dense scatter, UMAP projections, and GPU-resident high-cardinality plots.
- Do not use Datashader point rasterization for small discrete feature-time matrices; use true grid heatmaps with visible cells, colorbar, subtitles, and `t-0/t-1/...` labels.
- For US full-universe explainability on a 16GB GPU, do not put all sampled days on CUDA at once. Use row microbatching around 4 sampled days for `S≈16800`; measured 32-row explainability completed with ~8.9GB peak VRAM, while 8 rows without row microbatching reached the 16GB ceiling.
- Keep perturbation feature-time batches small for full-universe explainability. Larger perturb batches reduce Python loop count but were slower in practice: a 4-row smoke run with perturb batch 4 took much longer than perturb batch 1 because each forward became a worse large-batch attention workload.
- Cross-asset transmission should chunk both source symbols and sampled rows. Keep `source_chunk_size * row_chunk_size` bounded around 8 repeated rows for `S≈16800` unless a fresh VRAM profile proves more headroom.
- Project-owned cross-asset graph processing and graph explainability should default to cuGraph (`explain_cross_asset_graph_backend: cugraph`). Do not implement new project graph analytics with NetworkX. Keep `networkx` only as an environment/runtime dependency required by PyTorch `torch.compile`/functorch internals; removing it breaks compiled training.
- Static PNG chart labels should avoid CJK text unless a CJK-capable Matplotlib font is confirmed; use ASCII feature-group labels in plots and explain them in the Markdown report.

Walk-forward summary visualization rules:

- Do not recreate `walkforward_first_test_year_only.png`; delete stale copies when refreshing artifacts.
- Top-level walk-forward summary plots should include multiple first-test-year views, not only one equity curve.
- First-test-year summary visuals should use only each fold's first test year, even when the fold's test split contains all future years.
- Keep fold-level first-test-year return/risk, turnover, and concentration views visible so strategy behavior can be judged before later test years dominate the picture.

## Testing And Verification

Use focused tests after small changes, then broader tests when training/model/loss code changes.

Common commands:

```bash
source scripts/runtime_env.sh
run_fintech_python -m py_compile \
  stockagent/config.py \
  stockagent/training/trainer.py \
  stockagent/training/loss.py \
  stockagent/backtest/simulator.py

run_fintech_python -m pytest -q -s test
```

Known repo quirk:

- Prefer `run_fintech_python -m pytest -q -s test` for the formal test suite to
  keep collection scoped to the maintained test directory.

Model-specific tests:

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s \
  test/test_low_rank_market_transformer_portfolio.py \
  test/test_explainability_smoke.py
```

Loss/backtest consistency tests:

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s \
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

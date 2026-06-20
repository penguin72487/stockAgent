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
- Masked score/logit sentinels must be dtype-safe. Do not use fixed `-1e9`
  in model hotpaths, because it overflows FP16 AMP; use a representable finite
  mask fill such as `finite_mask_fill_value(scores)`.
- It is normal and desirable that some tensors remain FP32:
  - model parameters
  - input storage tensors
  - portfolio weights after tanh + L1 normalization
  - loss/backtest accumulation and numerically sensitive finance metrics
- Do not force the entire pipeline to permanent BF16 storage just to satisfy "BF16"; use AMP for compute and keep sensitive reductions stable.

## Data Panel Backend Contract

The full US Yahoo universe is a distinct data-processing regime. Benchmark it
with the actual full parquet directory instead of small synthetic subsets when
choosing defaults.

Rules:

- `scripts/benchmark_data_backends.py` is the reproducible scanner/benchmark for
  data-processing hotspots. Its active optimization scope is PyArrow plus Polars
  Lazy/Streaming; pandas is kept only as the compatibility/reference path.
- Do not add DuckDB or cuDF back to the panel/runtime benchmark set unless a new
  request explicitly reopens those candidates.
- For full US daily parquet (`16811` files, `S≈16811`) with
  `tradable_mode: tradable`, runtime `build_panel(... panel_backend="auto")`
  should select Polars Lazy when available, then PyArrow, then pandas. Explicit
  `panel_backend="polars"` is an alias for `polars_lazy`; explicit
  `panel_backend="polars_streaming"` is available for measurement but is not the
  current auto default.
- US Yahoo parquet files under `us_stocks` must keep `Trading_Volume` for
  trainable stock-like assets. If old `_DL`/delisted/archive parquet files are
  missing that column, treat them as schema-broken data: repair should normalize
  `_DL` records back to the base Yahoo symbol and remove unusable delisted
  schema-mismatch files instead of letting panel build fail on the first file.
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

## Current Main Model Contract

The active model is `transformer_base_portfolio`.

The active Transformer-base lookback-32 config is:

```yaml
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
  fused_log_utility_loss: true
  auto_batch_size: false
  allow_dynamic_symbols: false
  eval_model_chunk_rows: auto
  eval_backtest_chunk_rows: 512
  eval_backtest_chunk_rows_auto: true
  eval_auto_chunk_rows_cap: 64
  num_workers: 0
  backtest_compile: true
  backtest_compile_stateful: true
  backtest_compile_dynamic: false
  loss_type: log_utility

  transformer_base_portfolio:
    d_model: 32
    attention_mode: market_token
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
    temporal_pooling: last
    temporal_query_mode: last_only
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
    dynamic_latent_tokens: false
    dynamic_market_tokens: false
    dynamic_token_hidden_mult: 2
    dynamic_token_gate_init: 0.1
    dynamic_token_dropout: 0.0
    head_hidden_dim: 32
    head_layers: 1
    dropout: 0.1
    default_temperature: 1.0
    portfolio_mode: long_short
    max_full_tokens: 16384
    checkpoint_blocks: false
    return_aux: false
    return_aux_details: false
```

Notes:

- The scalable Transformer can be moved from complete to compact via `attention_mode`.
- Avoid `attention_mode: full` on a full market universe unless symbol count is small enough for the `max_full_tokens` guard.
- For large universes, prefer `latent` or `market_token`.
- `return_aux_details` is useful for explainability but can increase memory pressure during training. Prefer `false` for tight VRAM training and enable it for explainability runs when needed.
- The previous low-rank model remains available as `low_rank_market_transformer_portfolio`.
- Latest speed baseline for TW full universe (`S≈2304`) is `attention_mode: market_token`, `lookback: 32`, `batch_size_train: 32`, `batch_size_eval: 16`, and `temporal_pooling: last`.
- `batch_size_train: 32` improves steady-state epoch throughput versus 16 on the current benchmark, but first-epoch compile/warmup time is higher; use it for long training runs, and re-benchmark before reducing it.
- `temporal_pooling: last` is the active user preference. It is slightly faster than attention pooling but relies on temporal blocks to carry useful history into the final token; re-check validation/test metrics after changing it.
- For `temporal_pooling: last`, the active speed ablation uses `temporal_query_mode: last_only` to shrink the temporal autograd graph. `full_then_last` remains available as the more exact-ish path for validation ablations when metrics regress.
- The active speed ablation sets `qk_norm: false`, `dropout: 0.1`, and `dynamic_token_dropout: 0.0` to trim attention/FFN dropout and Q/K RMS-normalization autograd nodes. Treat this as a speed baseline, not proof that the regularized model is worse; re-check validation/test metrics before making investment-quality conclusions.
- `TransformerBasePortfolioModel.forward_from_panel(features, date_indices, mask, ...)` is the preferred lazy-window path for `WindowedSplitTensors`: it projects each unique panel date once and gathers projected `[B,L,S,D]` windows before running the same downstream temporal/market-token/score path. Preserve old `forward(x, mask, ...)` API compatibility.
- `TransformerBasePortfolioModel.forward_from_panel_slab(feature_slab, mask, ...)` is the compile-friendly fast path for contiguous lazy-window batches: pass `[B+lookback-1,S,F]` panel slabs and keep `date_indices` / gather metadata outside the compiled model graph. It must remain numerically equivalent to materialized windows and generic `forward_from_panel` for contiguous rows.
- Keep training `return_aux: false` and `return_aux_details: false` unless the objective explicitly needs aux tensors; enable aux for explainability/inference runs rather than the tight VRAM training path.
- The active `market_token` architecture should follow this low-complexity flow:
  - input `[B,L,S,F]` -> feature projection -> shared temporal encoder per stock -> `z_base [B,S,D]`
  - masked market summary is `mean + std + mean absolute dispersion`, shape `[B,3,D]`
  - when dynamic market tokens are enabled, they are `static_anchor + sigmoid(gate) * delta(summary)`, with `num_market_tokens` usually `4` or `6`; the current speed ablation sets `dynamic_market_tokens: false` and uses learned static market-token anchors
  - market tokens read stocks through cross-attention with stock masks
  - stocks read updated market tokens through cross-attention
  - stock-level market gate applies `z = RMSNorm(z_base_or_factor + sigmoid(g_i) * market_delta)`
  - portfolio head uses three scalar heads: `mu`, `sigma`, `confidence`
  - score is `mu / softplus(sigma) * sigmoid(confidence)`, then masked de-mean for long/short, then tanh + L1 portfolio normalization
- Keep `alpha_mu`, `risk_sigma`, `confidence`, `stock_market_gate`, `z_market_delta`, dynamic token queries/deltas/gates, and market summary parts available in aux outputs when detailed explainability is requested.

Modern Transformer module contract:

- Keep residual connections and Pre-Norm.
- Default modern block settings are `norm_type: rmsnorm`, `ffn_type: swiglu`, `qk_norm: true`, `rope_temporal: true`; the current speed ablation deliberately overrides `qk_norm: false`.
- Apply RoPE only to temporal attention by default. Do not apply RoPE over the stock axis unless stock order is deliberately made meaningful.
- Keep PyTorch SDPA/Flash path enabled and keep `sdpa_batch_limit` for large `batch * symbols` temporal attention.
- When dynamic latent/market tokens are enabled, they should be gated deltas around static token anchors:
  - `dynamic_token = static_query + sigmoid(gate) * input_conditioned_delta`
  - use market-summary inputs from masked stock embedding mean/std/dispersion
  - keep dynamic gates small at initialization, e.g. `dynamic_token_gate_init: 0.1`
- When `return_aux_details` is true, expose dynamic token query/delta/gate/summary tensors so explainability can detect token collapse, over-concentration, or strange liquidity/price-level rules.

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
- Keep `sdpa_batch_limit` enabled for large universes. Temporal attention flattens to `batch_size * symbols`; unchunked SDPA can hit CUDA `invalid argument` when that dimension is too large.
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
- Portfolio direction and sizing should use `tanh(score)` for signed direction followed by L1 normalization for gross exposure control.
- Do not use dual-branch softmax as the active long/short position calculator. Legacy `dual_branch_softmax` / `masked_softmax` names are now compatibility wrappers around tanh + L1 portfolio normalization.
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
- The active loss preference is `log_utility`: maximize annualized mean net log return from canonical `run_backtest_torch` outputs.
- `log_utility` must use fee-adjusted `backtest.strategy_returns`, after `buy_fee_rate` and `sell_fee_rate` have been applied.
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
- For long-year runs, re-check the latest artifact before optimizing. The run under
  `artifacts/train_2000-2001-...-2024/epoch_curve.jsonl` showed train time
  dominating epoch wall time, with CPU-to-GPU train tensor transfer larger than
  model forward. In that case, prioritize guarded GPU train tensor caching over
  test-curve work.
- Every epoch should account for train, validation, sampled test loss, curve test, curve plot, checkpoint, scheduler/progress, and any reporting work.
- Do not hide expensive work behind `val_interval_epochs > 1` or skip curve/test/plot work unless the user explicitly asks.
- Recent preference: sampled test loss only needs one fold per epoch to reduce epoch-level overhead.
- Keep curve plotting async where possible.
- When comparing throughput after compile, chunking, or cache changes, use the second epoch or later steady-state numbers. Do not choose defaults from the first epoch, because compile/autotune/warmup can dominate it.
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
- When invoking `/home/user/miniforge3/envs/fintech/bin/python` directly, PATH may not include the env `bin`. Compile helpers must prepend that path so Triton can find `/home/user/miniforge3/envs/fintech/bin/ptxas`.
- Compile cache paths should be stable and persistent across runs:
  - `TORCHINDUCTOR_CACHE_DIR=~/.cache/torchinductor`
  - `TRITON_CACHE_DIR=~/.cache/triton`
  - `CUDA_CACHE_PATH=~/.cache/nv_cuda`
  - do not delete these caches between repeated same-shape benchmarks unless explicitly testing cold compile behavior
- Current benchmark result for the active `data_okx` lookback32 run: compare only epoch 2 or later. The fastest measured compile combination is model compile plus fullgraph fused log-utility loss:
  - `enable_torch_compile: true`
  - `backtest_compile: true`
  - `backtest_compile_stateful: true`
  - `backtest_compile_dynamic: false` for fixed train/eval shapes
  - `fused_log_utility_loss: true`
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
- Trainer compile checks should discover `/home/user/miniforge3/envs/fintech/bin/ptxas` and the conda compilers `x86_64-conda-linux-gnu-gcc/g++` even when the parent shell PATH is sparse.
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
  - `fused_log_utility_loss: true`
  - `compile_loss: true`
  - only compile the fullgraph fused log-utility fast path; keep general `risk_aware_loss` as the debug/research path
- Eval model forward chunking and eval backtest chunking are intentionally decoupled:
  - keep model chunk sizing VRAM-driven, often `eval_model_chunk_rows: auto`
  - use larger `eval_backtest_chunk_rows`, currently `512`, to reduce `run_backtest_torch()` calls without skipping any val/test curve rows
  - preserve `prev_weights` continuation across backtest chunks and reset only at fold/segment boundaries
- `run_backtest_torch_reduced(..., reduction="log_utility")` exists for exact in-loop log utility reduction, but it is opt-in via `STOCKAGENT_LOSS_REDUCED_LOG_UTILITY=1` until a stateful compiled/Triton/C++ path benchmarks faster. The eager reduced loop was slower, and independent reduced runner compile warmup stalled in testing.

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

## Explainability Contract

The user wants detailed model explainability to detect strange rules and judge strategy trustworthiness.

Expected explainability workflow:

- `python explain_model.py` should default to drawing the full explainability set unless the user asks for a smaller run.
- Analyze all folds when making model-level claims.
- Keep `training.explain_after_each_fold: false` by default so training VRAM/time stays focused on train/eval/test artifacts.
- Generate explainability after training with `python explain_model.py`, which defaults to scanning all folds that have `checkpoint_best.pt`.
- Only enable `training.explain_after_each_fold: true` for deliberate smoke/debug runs, because paper explainability can be slow and VRAM-heavy.
- Default test explainability should use only each fold's first test year unless the user explicitly asks for all test years.
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
  - aux summaries for latent factors and market tokens
- Dense explainability plots should use RAPIDS/cuDF/Datashader when available.
- Dimensionality reduction for transformer aux tensors should use cuML UMAP, not PCA, for the default explainability projection path.
- Aux UMAP projection outputs live under `aux_projections/*.csv` and `plots/aux_umap/*.png`; use them to inspect stock embeddings, latent factors, market tokens, dynamic token deltas, and token collapse/regime clustering.
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

# Transformer-base architecture and historical benchmark

Use only for transformer_base_portfolio work or reproduction of this benchmark. The lookback, pooling, batch and compile settings below describe that experiment, not the repository-wide active baseline. Architecture interface and numerical-equivalence requirements still apply to this model. See models.md for baseline selection; benchmark again before transferring settings to another shape or device.

The historical Transformer-base lookback-32 benchmark config is:

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
- The recorded TW Transformer-base full-universe benchmark (`S≈2304`) used `attention_mode: market_token`, `lookback: 32`, `batch_size_train: 32`, `batch_size_eval: 16`, and `temporal_pooling: attention`.
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


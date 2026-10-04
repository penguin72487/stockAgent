# Precision and model defaults

Use when selecting or changing a model, precision, or a new experiment baseline. Resolve the selected configuration and its inheritance first. Product-specific contracts and the latest explicit experiment settings govern. Historical config examples and measured timings are not universal model restrictions.

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

## Current Main Model Recommendation

### Default Training Baseline And Immediate Fold Reports

Unless the user explicitly selects another baseline, new strategy training
configs use the architecture/data baseline in
`configs/markets/tw_day_trade_daily_multi_basis_projection_l1_tplus2_close_capital10m.yaml`
and its completed OFAT control at
`artifacts/ablations/tw_day_trade_daily_multi_basis_projection_l1_tplus2_close_commission20_panel_history_v3_ofat_capital10m/baseline`
as the default training standard. Inherit its model, multi-basis inputs,
TWD 10M execution capital, panel-history walk-forward,
BF16/DDP, fee, settlement, 1000-epoch, and epoch-curve settings unless the
strategy or the user explicitly overrides a field. Product-specific data,
execution, eligibility, settlement, and accounting contracts remain
authoritative and must not be replaced by Taiwan stock-day-trade semantics.

For new single-target Taiwan stock day-trade training, the historical
`projection_l1` output is no longer the active policy contract. Use
`portfolio_output_mode: learned_cash` with
`center_long_short_logits: false`, as pinned by
`configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml`.
This mode uses a zero-initialized contextual cash gate to scale a signed
unit-L1 stock direction, so the model—not failed fills, whole-lot rounding, or
volume capacity—chooses long gross, short gross, net exposure, total gross, and
cash. Candidate count and arbitrary stock-logit scale therefore cannot force
gross to one. Do not substitute legacy `cash_l1`: its one cash score still
competes against an unscaled sum over S stocks. Preserve old projection/cash-L1
code and configs only for exact historical artifact replay;
do not start new stock day-trade experiments from them. Product-specific models
whose validated action ABI still requires projection-L1 are not silently
migrated by this stock-policy rule.

For full-stock-context nearby single-stock-futures day trade, the product-
specific default is
`configs/markets/tw_stock_futures_day_trade_0845_minute.yaml`.
It inherits the common 1000-epoch/BF16/walk-forward standard and all 98
prior-completed stock features. The 08:45 daily decision excludes the 09:00
cash-stock opening gap; the 08:46 right-labelled futures bar owns entry.
At 13:20 place a passive limit, replace it at 13:24 with market exits through
the 13:30 deadline. Preserve whole contracts, per-minute capacity, futures
costs, causal candidate masks, and residual cash. In this strict v1 config,
missing minute history must fail closed, never substitute a daily OPEN/CLOSE label. Earlier 08:45 daily
v1/v2, 09:00 sidecar v3, and 09:00 daily proxy v4/v5 remain incompatible
reproducibility controls. See `docs/tw_stock_futures_day_trade_minute.md`.

The user's 2026-09-07 historical-data request additionally authorizes
`configs/markets/tw_stock_futures_day_trade_0845_historical.yaml`: contract v2
uses official same-physical-contract daily OPEN/CLOSE for unavailable early
minutes strictly before `tw_stock_futures_day_trade_daily_proxy_before`
(currently `2020-01-01`). On/after that cutoff the existing minute clock and
missing-source rejection remain mandatory. A daily CLOSE is not a proven
13:30 fill. Keep regime channels, cutoff, benchmark/terminal reporting, and
checkpoint fingerprints explicit; reuse the common trainer and integer basket.
Do not silently extend the cutoff to Shioaji's 2020-03-22 history boundary.

The user's 2026-09-08 follow-up narrows this historical experiment to Shioaji's
available period: `data.panel_start_date: 2020-03-23` (first day session) and
`walk_forward.expected_first_year: 2020`. Preserve earlier source data and the
v2 cutoff, but include no early daily-proxy rows in this experiment. Use a new
prepared-data version and artifact root; all later missing-source and physical-
identity checks still apply. This changes the requested horizon, not the
08:45/08:46/13:20/13:24/13:30 execution clock.

The user's 2026-09-09 correction replaces whole-session source quarantine with
explicit physical-contract-day quarantine. For the historical experiment only,
`tw_stock_futures_day_trade_quarantine_contract_days` excludes
`2021-06-21 / LVF:202107`; `tw_stock_futures_day_trade_quarantine_dates` is empty.
Preserve every other candidate, original candidate slots, the complete decision
and stock-feature calendars, and raw unresolved evidence. This is retrospective
source-quality scope, not a claim about historical market eligibility. Never
infer additional exclusions from gaps or failed strategy exits. Manifest,
checkpoint, reporting, and a new artifact root must bind the exact scope.

Receipt-backed one-minute KBars are sufficient where their dated physical
identity and Amount/Volume contract verify; preserve the existing --minute-root
and --kbars-only collector paths. Validate source integrity before the stock
panel or DDP. A complete receipt covers only declared dates, not the entire
configured history. Use train.py --config ... --check-data-only for coverage
and scripts/run_data_cache.sh for exact-release verification and leases.
The user's current direct train.py entry point remains authoritative.

Every completed fold must immediately refresh the cumulative root-level
walk-forward report from all contract-compatible folds completed so far. Do not
wait for the full fold suite and do not require a duplicate post-training
inference pass. The required plot set is:

- `walkforward_equity_curve.png`
- `walkforward_first_year_cumulative_returns_log10.png`
- `walkforward_first_year_cumulative_returns.png`
- `walkforward_first_year_fold_metrics.png`
- `walkforward_first_year_turnover_concentration.png`
- `walkforward_stitched_deployment_cumulative_returns_log10.png`
- `walkforward_stitched_deployment_cumulative_returns.png`
- `walkforward_stitched_deployment_fold_metrics.png`
- `walkforward_stitched_deployment_turnover_concentration.png`

For isolated-fold training, the orchestration parent must reload all completed
fold results and refresh these plots after each child succeeds; a one-fold child
must not overwrite the cumulative report with only its local fold.

The default active model is `financial_transformer`. Use another model only
when the user or a strategy config explicitly overrides `training.model_name`.
`transformer_base_portfolio` remains available as an explicit scalable-model
alternative, but it is not the default baseline.

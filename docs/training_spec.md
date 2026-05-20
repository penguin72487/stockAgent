# Training Specification

## Objective

Train a Taiwan equity multi-asset trading system that can trade the full daily stock universe, evaluate against the same-day average return of the full stock universe, and scale training on CUDA with Tensor Core acceleration.

## Dataset assumptions

- Input source: `data_parquet/*.parquet`
- Each file is one symbol with daily features.
- Representative schema currently includes:
  - `date`
  - `open`, `max`, `min`, `close`
  - `Trading_Volume`
  - `PER`, `ROE`, `Debt_Ratio`
  - `PER_Z_Score`, `Debt_Ratio_Scaled`
- The trading universe is the full daily stock pool, not a fixed subset.
- A per-date tradability mask must be constructed because symbols appear and disappear over time.

## Baseline

- Primary benchmark: the same-day cross-sectional average return of all tradable stocks.
- Benchmark construction rule: on each trading day, compute the arithmetic mean of all tradable stock returns in the active universe.
- No external benchmark file is required for the primary baseline because it is derived from the daily stock panel.

## Walk-forward validation

Use yearly expanding-window validation.

Definition:

- Fold 1: train year 1, validate year 2, test years 3 to end
- Fold 2: train years 1 to 2, validate year 3, test years 4 to end
- Fold 3: train years 1 to 3, validate year 4, test years 5 to end
- Continue until the final validation year still leaves at least one test year

Operational rules:

- Hyperparameter selection is based on validation results only.
- Test results are reported out of sample for each fold.
- Aggregate performance should include per-fold metrics and a stitched equity curve from non-overlapping selected test windows if a final production score is needed.

## Trading assumptions

- Frequency: daily
- Trading universe: all tradable stocks on each trading day
- Transaction fee: `0.001` per side
- If transaction tax, slippage, or limit-up and limit-down constraints are needed, they should be modeled explicitly in the simulator instead of being folded into the fee field.
- Symbols that are not tradable on a given day must be masked from action generation and portfolio construction.

## Recommended modeling roadmap

### Stage 1: panel builder and simulator

Build a shared panel with these core tensors:

- `features[t, s, f]`
- `returns_1d[t, s]`
- `returns_5d[t, s]`
- `tradable_mask[t, s]`
- `alive_mask[t, s]`

This stage is required before any model training.

### Stage 2: supervised cross-sectional baseline

Train a GPU model that scores all symbols each day.

Recommended first target:

- Predict next-day or next-5-day return ranking

Recommended first model families:

- MLP on per-symbol features
- Temporal model with short rolling window if sequence structure is needed later

Portfolio rule for baseline evaluation:

- Rank all tradable stocks each day
- Hold top-k names or convert scores to long-only weights
- Compare against the same-day universe average return baseline and against a simple equal-weight version

### Stage 3: portfolio policy layer

Before RL, add a policy layer that converts alpha scores into weights under constraints.

Suggested constraints:

- long-only
- cash allowed
- single-name cap
- turnover penalty
- optional sector cap

### Stage 4: RL policy

Use RL only after the supervised baseline and simulator are stable.

Recommended RL action space:

- target portfolio weights for all tradable symbols

Recommended reward:

- daily pnl
- minus transaction fee
- minus turnover penalty
- minus concentration penalty

## CUDA and Tensor Core plan

Use PyTorch as the main training backend.

Recommended settings:

- device: `cuda`
- mixed precision: `bf16` if supported, otherwise `fp16`
- enable Tensor Core friendly matrix dimensions when batching
- use `torch.autocast` and `GradScaler` when needed
- set DataLoader `pin_memory=True`
- use non-blocking host-to-device copies
- keep tensors in contiguous dense layout for batched daily cross-sectional training

Recommended tensor layout for fast training:

- batch over time windows
- process all symbols in parallel within a batch
- keep features as dense tensors plus masks instead of Python loops over files

Example shapes:

- `x`: `[batch, lookback, symbols, features]`
- `mask`: `[batch, symbols]` or `[batch, lookback, symbols]`
- `y`: `[batch, symbols]`

## Metrics

Minimum metrics to report per fold:

- cumulative return
- annualized return
- Sharpe ratio
- max drawdown
- turnover
- daily hit rate
- excess return versus universe average

## Open implementation requirements

- define the exact tradable-mask rule used to compute the universe-average benchmark
- define whether transaction tax is included separately from fee
- define slippage model
- define top-k versus continuous weight portfolio output
- define whether the first release is long-only or supports shorting
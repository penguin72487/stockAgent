# Fold 11 combined annual/sub-lot result and cash-output follow-ups

## Scope and evidence boundary

The completed artifact is
`artifacts/markets/tw_day_trade_v8_combined_annual_output_v2_sub_lot_fold11_v1`.
It is one isolated fold: training years 2014--2024, validation year 2025 and an
already-inspected 2026 test interval from 2026-01-02 through 2026-09-11.  The
run used the pinned 99-feature, 2,754-symbol public panel and physical execution
release `tw-day-trade-carry:69d2c7dd...8882b80f`.  Its completion receipts,
terminal account and settlement audit are valid.  The result is useful for
mechanism diagnosis; repeated changes selected on 2026 cannot turn that same
interval into a new blind test.

## What the result says

| Metric | Completed v1 | Earlier matched v8 baseline |
|---|---:|---:|
| 2025 validation return | +66.13% | +60.45% |
| 2025 validation Sharpe | 2.525 | 3.192 |
| 2026 test return | +40.81% | +32.11% |
| 2026 test Sharpe | 1.919 | 2.853 |
| 2026 maximum drawdown | -11.28% | not used for the output diagnosis |
| 2026 excess versus 2330 close-to-close comparator | -15.59 percentage points | -24.29 percentage points |
| 2026 official-open/close proxy IC | -0.0574 | -0.0504 |
| Median requested gross | 75.39% | 59.10% |
| Median largest requested name | 24.12% | 12.03% |
| Median effective name count | 4.94 | 10.26 |

The combined annual episodes and sub-lot recovery improved terminal return,
but they also approximately doubled the median largest position and halved the
effective number of names.  The equity curve's main weakness is the
mid-May-to-mid-June drawdown, followed by a discontinuous recovery in late
June.  This is consistent with a small number of names controlling daily PnL,
not with a dead portfolio or missing fills.

The selected checkpoint came from epoch 241.  Training stopped at epoch 341
after 100 validation checks without improvement.  The final epoch continued to
improve training loss (`-3.6061`) while validation was worse than the selected
epoch (`-0.3519` versus `-0.5264`).  Gradients remained finite and no training
batch had zero gradient, so a longer epoch ceiling alone does not repair the
observed generalization gap.

## Direction, cash and execution diagnosis

The saved requested weights show:

- 153 of 168 test sessions were net short and 15 were net long.
- Median short gross was 75.39%; median residual cash was 24.61%.
- 90.36% of all requested gross was short.
- Median effective name count was 4.94, even though the median nonzero request
  count was 14.5.  Many requests therefore carried negligible weight beside a
  few dominant names.
- 58.52% of nonzero requests were smaller than one board lot, but they were
  only 11.62% of requested gross.  Open permission failures blocked 7.06% of
  requested gross and daily proxy sessions represented 5.37%.

The broad short bias is not by itself a sign error.  Before fees and capacity,
the mean legal-universe open-to-close log return was -15.20 bps/day in the
training interval, -17.10 bps/day in validation and -21.63 bps/day in the
inspected test interval.  A short intraday market component was therefore
profitable in all three periods.

The negative IC is not an execution-matched stock-selection verdict.  It uses
the panel's official-open/close return component, while authoritative PnL uses
the physical 09:01 entry and 13:20 limit / 13:24 market / 13:30 terminal exit
path.  Validation and test proxy IC are both negative, which proves that saved
weights oppose the official-open/close cross-section; it does not prove that
they oppose the later physical fills that trained the model.  Adding an IC
penalty from this mismatched label would optimize a second execution clock and
is therefore not part of the correction.  The 2330 comparator covers prior
close to current close and includes an overnight component that the strategy
does not hold.  It remains the project's formal comparator, but its +56.41%
test return does not prove that the day-trade policy should have been net long
intraday.

## First-principles output correction

For the completed `score_entmax_cash_v2` output, let `z_i` be a legal signed
score, `a_i = abs(z_i)` and `p = entmax15(a)`.  Its magnitude is

```
abs(w_i) = p_i * a_i / (1 + a_i)
```

One raw magnitude controls both `p_i` and the per-leg conviction.  An outlier
therefore wins the allocation twice.  Annual exact-account optimization and
the board-lot recovery gradient reward a score that crosses a lot threshold;
that creates a direct feedback path toward the observed concentration.

The new `score_entmax_bounded_cash` map uses

```
c_i = a_i / (1 + a_i)
p   = entmax15(c)
w_i = p_i * z_i / (1 + a_i)
cash = 1 - sum(abs(w_i))
```

This has the required contracts:

1. No cash token, output head, exposure target, top-K or new trainable
   parameter is introduced.
2. Score sign still chooses long or short.  Score scale still changes risky
   gross and cash.
3. Zero scores produce all cash and retain a finite nonzero gradient.
4. Legal masks are applied before allocation, output remains FP32 under BF16,
   and requested gross cannot exceed one.
5. Because selector inputs lie in `[0, 1)`, raw score scale cannot create an
   unbounded entmax gap.  In a 2,754-stock deterministic check with one score
   equal to 5 and all others zero, the historical output puts more than 80% in
   that name; the bounded output requests less than 20%.

This specifically repairs the concentration mechanism.  It does not claim to
make relative stock forecasts correct.  Acceptance still requires a fresh
training run selected on 2025 validation, followed by inspection of exact 2026
account results.  The minimum success condition is improved validation Sharpe
and concentration without losing the v1 validation return; a future unseen
period is required for a new generalization claim.

## Implemented follow-up

The follow-up config is
`configs/deployments/tw_day_trade_v8_combined_annual_bounded_cash_sub_lot_fold11_v2.yaml`.
It inherits the completed v1 configuration and changes only experiment/root
identity and the output contract.  It retains 1,000 epochs, 0.1 early-stop
ratio, annual training episodes, sub-lot recovery, the exact FIFO account,
BF16/DDP and the pinned dataset.  Its artifact root is independent, so it
cannot resume the incompatible v1 action/checkpoint ABI.

Run it with:

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONUNBUFFERED=1
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_v8_combined_annual_bounded_cash_sub_lot_fold11_v2.yaml \
  --start-fold 11 --max-folds 1 --resume --profile-timing
```

## Observed v2 result and corrected interpretation

The v2 training lifecycle did not reach early stopping.  It received
`SystemExit(130)` before epoch 96.  Its best checkpoint is epoch 64; the last
recorded epoch had only 31 validation checks without improvement, below the
configured 100-check early-stop patience.  A pure inference replay of epoch 64
produced the following exact FIFO account result:

| Metric | v1 raw selector | v2 bounded selector |
|---|---:|---:|
| 2025 validation return | +66.13% | +26.80% |
| 2025 validation Sharpe | 2.525 | 2.769 |
| 2025 validation Sortino | 4.301 | 5.375 |
| 2025 validation MDD | -11.57% | -3.34% |
| 2026 inspected return | +40.81% | +21.29% |
| 2026 inspected Sharpe | 1.919 | 3.464 |
| 2026 inspected Sortino | 3.137 | 6.961 |
| 2026 inspected MDD | -11.28% | -2.10% |
| Median requested gross | 75.39% | 49.66% |
| Median largest requested name | 24.12% | 6.02% |
| Median effective name count | 4.94 | 15.81 |

The bounded map therefore did improve risk-adjusted performance and drawdown;
its failure is underinvestment and lower terminal return.  At the same epoch
95, its train/validation losses were `-1.320/-0.214`, versus
`-2.888/-0.410` for v1.  More epochs could still change v2, but the same-epoch
gap plus the exposure geometry show that its selector is structurally too flat
for this wide, board-lot-constrained account.

Pure inference also exposed two artifact bugs unrelated to strategy quality.
The neural and tree inference finalizers omitted the physical FIFO source
context when saving the NPZ and omitted `test_backtest_symbols.json` before
walk-forward replay.  Training finalization already supplied both.  The
inference paths now use the same ordered universe, immutable source release,
segment initial NAV and symbol sidecar contract.

## Parameter-free logarithmic selector

The next output geometry keeps the useful parts of both results.  With
`a_i = abs(z_i)` it computes

```
s_i = log(1 + a_i)
p   = entmax15(s)
w_i = p_i * z_i / (1 + a_i)
cash = 1 - sum(abs(w_i))
```

`log1p(a)` has slope one at zero, so weak scores and the all-cash initial state
remain trainable.  It is unbounded, allowing genuinely separated names to
leave the entmax support, while its logarithmic tail prevents a raw outlier
from widening the selector gap linearly.  The per-leg conviction is unchanged,
so the model score still learns risky gross and residual cash.  The map adds no
parameter, cash token, top-k rule or target exposure.

The new config is
`configs/deployments/tw_day_trade_v8_combined_annual_log_cash_sub_lot_fold11_v3.yaml`
with independent root
`artifacts/markets/tw_day_trade_v8_combined_annual_log_cash_sub_lot_fold11_v3`.
It inherits v1's validated data, 1,000 epochs, early stopping, annual episodes,
sub-lot backward recovery and exact physical FIFO execution.  Its checkpoint
contract is `score_entmax_log_cash_v1`, so it cannot resume either v1 or v2.

Run it with:

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONUNBUFFERED=1
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_v8_combined_annual_log_cash_sub_lot_fold11_v3.yaml \
  --start-fold 11 --max-folds 1 --resume --profile-timing
```

## 2026-09-25 annual boundary correction

The validated panel starts on 2014-01-06 and the model lookback is 32 sessions.
With panel-history context, the 2015--2024 accounts already started on each
year's first panel trading session. The first 2014 account did not: its first
model decision was 2014-02-27, so treating it as a complete calendar-year
fresh-capital account mixed feature warmup with the account horizon.

The corrected configuration is
`configs/deployments/tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_fold10_v4.yaml`.
It retains 2014 in the panel only as causal lookback history, starts target-year
ownership in 2015, trains complete 2015--2024 annual accounts, validates on
2025, and keeps the already-inspected 2026 test interval. The new artifact root
and `calendar_year_first_session_fresh_capital_v2` continuation contract prevent
the old optimizer state from resuming under the corrected accounting horizon.

Training now fails before optimizer creation unless every target year contains
exactly all owned sessions from that year's first through last trading day. The
physical FIFO binding repeats the first-session check, and a formal run writes
`annual_episode_boundaries.json` as the per-year receipt.

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONUNBUFFERED=1
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_fold10_v4.yaml \
  --start-fold 10 --max-folds 1 \
  --multi-gpu-strategy distributed_data_parallel \
  --resume --profile-timing
```

To train the complete corrected walk-forward sequence from fold 1, use
`configs/deployments/tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_all_folds_v5.yaml`.
Corrected fold 1 has `train=2015`, `validation=2016`; corrected fold 10 has
`train=2015--2024`, `validation=2025`. Each target fold independently imports
the model state from the unique source fold with the same validation year and
only the additional earlier 2014 training prefix. Optimizer state and source
test metrics remain excluded, and the epoch-zero exact-account validation guard
remains active.

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONUNBUFFERED=1
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_all_folds_v5.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --resume --profile-timing
```

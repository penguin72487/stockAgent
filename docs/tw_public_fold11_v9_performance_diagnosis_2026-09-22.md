# Fold 11 v9 performance diagnosis (2026-09-22)

The supplied `walkforward_equity_curve.png` is a **single completed fold 11**
deployment, covering 172 sessions from 2026-01-02 through 2026-09-17. It is not
evidence for all walk-forward folds. The stitched deployment return is -2.22%
with a -10.03% maximum drawdown; the 2330 comparison is +57.85%. The fold's
separate test metric is -1.82%, so the deployment and fold-test figures should
not be interchanged. A buy-and-hold 2330 position is not the same risk or
execution contract as an intraday long/short strategy; the strategy also fell
below its zero-return cash reference.

## Matched evidence

The completed v4 `score_entmax_cash` fold artifact and v9 use the same dataset
fingerprint `85a854ce654e917bad14af586d0a24334e107c2bd0f588f063ecfbf941345b60`
(3,100 sessions, 2,755 stocks, 428 features), the same fold and physical FIFO
execution settings. Their effective configurations differ in output mode and an
unused projection-L1 scale switch, besides the experiment name and output root.
The v4 root's later `progress.json` says `failed`; the comparison below uses its
earlier `fold_11/fold_complete.json` marked `complete` and its fold artifacts.

| Measured quantity | v4 `score_entmax_cash` | v9 `score_entmax_scale_separated_cash` |
|---|---:|---:|
| 2025 validation return / Sharpe / maximum drawdown | +13.20% / 1.723 / -5.22% | +12.65% / 1.055 / -9.44% |
| 2025 validation turnover | 0.412 | 0.591 |
| 2026 fold-test return / Sharpe / maximum drawdown | +4.79% / 0.725 / -5.38% | -1.82% / -0.204 / -10.03% |
| 2026 stitched deployment return | +4.59% | -2.22% |
| 2026 mean exact fill turnover | 0.380 | 0.494 |
| 2026 median requested gross / gross standard deviation | 0.437 / 0.078 | 0.449 / 0.023 |
| 2026 short share of total requested gross | 94.0% | 99.8% |
| 2026 median effective stock count | 11.54 | 6.38 |

The observed 2026 difference is not a precision estimate of future alpha. On
the 172 paired daily `strategy_returns` values, v4 minus v9 averages 0.000379
per session; a 10,000-replicate circular five-session block bootstrap gives a
95% interval of [-0.000670, 0.001452] per session, which includes zero. This
exploratory interval assumes the observed period is representative and should
not be used to select a mode on the already inspected test year.

Effective count is `(sum(abs(w)))**2 / sum(w**2)` on requested weights. In v9,
170/172 days requested more than 95% of gross on the short side, and 169/172
days requested less than 1% long gross. The daily requested gross stayed close
to 0.45 (5th–95th percentile 0.413–0.483). This is evidence that the learned
policy used the output's cash freedom only weakly and became a concentrated
short basket. The all-zero end-of-session `weights_history` and `shares_history`
are required by terminal day-trade liquidation; they do **not** mean there were
no fills. Nonzero exact fill turnover occurred on 171 v9 test days.

V9's output map computes `p = entmax_1.5(abs(z) / RMS(abs(z)))` and
`q = sum(p * abs(z))`, then emits `w_i = p_i*z_i/(1+q)`. Thus requested gross is
`q/(1+q)`: score scale can change cash without a new trainable parameter, but
the map cannot make the underlying scores predict profitable direction or
exposure. Its RMS-normalized selector made this checkpoint more concentrated
than the matched v4 checkpoint. Sparse support is a property of entmax, not a
profit guarantee ([Peters, Niculae, and Martins, ACL 2019](https://aclanthology.org/P19-1146/)).
Higher filled turnover raises the cost burden under the existing commission,
short-handling fee, sell tax, and slippage contract; these saved outputs do not
separate the exact incremental fee cost from changed stock picks. Fixed and
proportional costs can change the optimal portfolio materially
([Lobo, Fazel, and Boyd, 2007](https://stanford.edu/~boyd/papers/portfolio.html)).
Do not attribute the entire return difference to any one of short direction,
sparsity, or fees without a matched replay.

## Training and data limits

The v9 name says `10240`, but its `base_config` chain reaches v6, whose
effective `training.epochs` is **1024**. At an early-stop ratio of 0.1 this
means 103 checks of patience. The epoch curve has 209 epochs: the best 2025
validation loss occurred at epoch 106 (-0.1235), and training stopped after
103 checks without a new best. Training loss kept falling from -0.0149 to
-0.7463 while validation ended at +0.0167. This supports a generalization
problem; raising the epoch ceiling alone has no demonstrated benefit.

The 428-channel research release is pinned and its `--check-data-only` proof
passes, but `historical_vintage_verified=false`, daily minute proxies are
allowed, and the physical-data receipt retains 563,949 unresolved corporate-
action-gap symbol-days. These are limitations on live interpretation, not a
newly discovered dataset failure. The old OFAT baseline used different source
releases, feature visibility, pretrained initialization and batch size, so its
stronger 2026 result is **not** an output-mode-only control.

## Bounded next experiment

`configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_cpu_cache_score_entmax_cash_10240_v10.yaml`
inherits the matched v4 mode and data, with a distinct artifact root. It sets
an actual 10,240-epoch ceiling and ratio 0.01, giving the same 103-check
patience as the historical v9 run. It adds no model parameter, manual exposure
cap, or alternative execution. `load_config` confirms only experiment/root,
epochs, and early-stop ratio differ from v4; `train.py --check-data-only` passed
for fold 11 without constructing a model or checkpoint. Full v10 training has
not been run. A changed ceiling also changes the warmup-cosine schedule, so a
future v10 result is a new training experiment, not an identical v4 replay.
The strict environment precheck also passed with two RTX 5090 GPUs, no reported
warnings, and no reported failures.
Because this choice was made after inspecting the 2026 results, v10 is
exploratory: its 2026 result cannot be presented as a blind test or as proof of
improvement over v9.

Use 2025 validation return **together with** Sharpe, drawdown, turnover,
requested gross variability, directional share and effective count to decide
whether a candidate merits further testing. The 2026 path has already been
inspected repeatedly; do not use it to tune another output map or repeatedly
select checkpoints. Future generalization claims need additional chronological
folds or genuinely later data. Keep the exact physical fee/lot/capacity ledger
as the final accounting authority. Avoid imposing a fixed long/short or cash
fraction in response to this one adverse period; that would replace learning
with a hand-set exposure rule.

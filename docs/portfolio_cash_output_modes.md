# Cash-aware portfolio output contracts

For a signed target vector `w`, the **requested** risky gross is
`sum(abs(w_i))` and requested cash is `1 - sum(abs(w_i))`. These are policy
outputs before the whole-lot, fee, eligibility, volume, and fill rules. Cash
left after those rules is an execution result, not evidence that the model
chose cash. A model output mode must keep `sum(abs(w_i)) <= 1`, apply causal
eligibility masks before allocation, and leave failed orders unredistributed.

## Parameter-free score modes

`score_entmax_log_cash` is the current follow-up for the wide Taiwan
board-lot account.  It uses `p = entmax15(log1p(abs(z)))` and
`w_i = p_i*z_i/(1+abs(z_i))`.  The selector has unit slope at zero and an
unbounded logarithmic tail: it can become sparse when evidence is genuinely
separated, but raw outliers widen the selector gap much more slowly than in
`score_entmax_cash_v2`.  It has no cash head or additional trainable parameter,
keeps FP32 allocation under AMP, is all cash at zero scores, and preserves a
nonzero zero-score gradient.  Its checkpoint contract is
`score_entmax_log_cash_v1`.

`score_entmax_bounded_cash` is the follow-up for the completed combined
annual/sub-lot fold-11 run.  It maps legal magnitude to bounded conviction
`c_i = abs(z_i)/(1+abs(z_i))`, uses `p = entmax15(c)`, and emits
`w_i = p_i*z_i/(1+abs(z_i))`.  This prevents an unbounded raw outlier from
sharpening the selector and then receiving a second unbounded magnitude
advantage.  It adds no trainable parameter; exact-zero scores are all cash
with a finite nonzero gradient.  The diagnosis, derivation and runnable config
are in
[the combined fold-11 analysis](tw_day_trade_v8_combined_fold11_analysis_2026-09-24.md).
The observed v2 checkpoint confirmed the tradeoff: it improved test Sharpe,
Sortino and MDD but reduced median requested gross and terminal return.  The
logarithmic mode is the next fresh-root candidate between the bounded and raw
selector geometries.

`score_entmax_scale_separated_cash` was the earlier fold-11 research candidate.
It normalizes legal score magnitudes by their cross-sectional RMS before
entmax, then uses the unscaled scores for the shared risky budget. A common
positive score rescaling can therefore change requested cash without also
changing relative stock selection. Its derivation, exact-account motivation,
tests, and runnable v9 config are in
[the scale-separated cash-output note](score_entmax_scale_separated_cash.md).

`score_entmax_global_cash` is a new output/checkpoint contract. For each legal
stock score `z_i`, let `p = entmax15(abs(z))` over the legal candidates,
`q = sum(p_i * abs(z_i))`, and `w_i = p_i * z_i / (1 + q)`. A short-prohibited
stock uses `max(z_i, 0)` in place of `z_i`. The common requested gross is
`q / (1 + q)`; the remainder is cash. Entmax gives sparse relative allocations,
while the shared denominator gives one portfolio budget without an extra cash
token, output head, or trainable parameter. At `z = 0`, the portfolio is exactly
cash, yet `d w_i / d z_i = p_i` for legal signed scores, so the normalizer does
not trap an exactly flat initialized head. Calculations and emitted weights
stay FP32 under BF16/FP16 autocast for the exact whole-lot ledger. The model
checkpoint contract records `score_entmax_global_cash_v1`. Both new modes are
restricted to single-target heads; multi-phase carrying actions need their own
validated allocation contract.

This is an output inductive bias, not a parameter-free claim about the whole
model. The `1` in `1 + q` is a fixed score-to-budget calibration, and the
entmax exponent is fixed at 1.5. Increasing a common score scale also changes
entmax sparsity, so selection and gross are not fully independent. Adding or
removing many near-tied legal stocks can change relative allocations; this mode
does not promise exact invariance to arbitrary universe changes. An exact
integer executor may also have a separate minimum-lot gradient or sizing
threshold; the nonzero normalizer gradient alone does not prove executed trades.

`score_entmax_cash_v2` retains the historical `score_entmax_cash` **forward**
formula `w_i = p_i * sign(z_i) * abs(z_i)/(1 + abs(z_i))`, but evaluates the
signed factor as `z_i/(1 + abs(z_i))`. That algebraic form restores its derivative
at exact zero scores. It has a separate output/checkpoint contract because
training from a flat head can now follow a different optimizer trajectory.
The historical `score_entmax_cash` and `cash_entmax15` modes remain unchanged
for exact replay. Both v2 and the global mode return FP32 weights.

The historical fold-11 comparison configs inherit the same pinned observed
data, physical FIFO account, model backbone and optimizer.  Despite `10240` in
their file names, the effective inherited ceiling for v7--v9 is 1,024 epochs;
the output contract and artifact root differ:

- `configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_cpu_cache_score_entmax_global_cash_10240_v7.yaml`
- `configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_cpu_cache_score_entmax_cash_v2_10240_v8.yaml`

They are separate experiments. Do not resume either from the earlier
`projection_l1` or `score_entmax_cash` optimizer checkpoint. No strategy
performance claim follows from the allocation identities or unit tests.

## Existing output choices

| Mode | Cash mechanism | Limitation relevant to interpretation |
| --- | --- | --- |
| `learned_cash` | Independent contextual cash gate scales a signed unit-L1 stock direction | Adds a trainable cash token, normalization layer, and score head. It has the clearest independent budget but a distinct parameter/checkpoint ABI. |
| `projection_l1` | Direct scores are projected into the unit L1 ball | Once the raw L1 norm exceeds one, requested gross is pinned at one; gross cannot learn from larger common score magnitudes in that region. `projection_l1_scale_by_active_count` mitigates width-driven saturation but does not remove the boundary. |
| `score_entmax_cash` | Each selected stock has its own bounded conviction | Gross is a weighted average of per-stock convictions, and the historical `sign * abs` expression has zero score gradient at the exactly flat state. Use v2 for a fresh run that requires that gradient. |
| `signed_softmax`, `signed_sparsemax`, `signed_entmax15` | One explicit cash action competes with long/short actions for every stock | Its probability depends on action count; simultaneously allocated long and short actions can cancel. `action_cash_alloc` reports chosen cash, `action_cancellation_cash` reports the cancellation contribution, and `implicit_cash_weight` reports their sum. |
| `cash_l1`, `l1`, `activation_l1` | Legacy cash score or forced normalization | `cash_l1` makes one cash score compete with a sum over all stock magnitudes. The L1-normalized modes request full gross whenever there is a nonzero legal score. |

Compare modes using requested gross/cash and executed gross/cash separately,
with legal candidate count, zero-score gradient, held-out loss, exact lot
thresholds, fees, and checkpoint fingerprints visible. No output map can be
assumption-free: without a separate cash output, a fixed rule must translate
the stock-score scale into a risky budget.

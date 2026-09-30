# Portfolio, backtest and loss contracts

Use for portfolio outputs, execution clocks, fills, costs, settlement, cash, or loss accounting. Apply each rule to its named product and execution mode. Historical reproduction, paper assumptions and live executable evidence remain distinct. For the stock-futures v2-v15 experiments also consult futures_history.md.

## Portfolio Direction Intent

The active baseline should support long/short portfolio weights when the user asks
for multi-directional trading. Earlier experiments used long-only; always follow
the latest explicit user intent and keep model, loss, and backtest settings
aligned.

Guidelines:

- The active `tw_day_trade_*` live paper modes start signal generation at 09:00.
  Once the immutable signal is published, value each independently constrained
  submitted whole-lot buy/cover at the first strictly later best Ask and each
  sell/short at the first strictly later best Bid. Missing causal quotes fail
  closed; never substitute the 09:01 price, last price, or an adverse `+1 tick`.
  Historical replay/backfill is a separate counterfactual contract: the
  official 09:00 session open is the model input and whole-lot sizing price,
  while the paper transaction is recorded at 09:01 from that completed
  right-labelled minute. Prefer `Amount / volume_shares`; when no usable tick or
  VWAP exists, use the same source-published 09:01 KBar `Close`. Missing ticks
  alone must not create a no-fill. Only an entirely missing/invalid 09:01 bar
  remains a visible price gap; never substitute the 09:00 open, a carried last
  price, Bid/Ask, or an adverse tick. Record whether the method was
  `minute_vwap` or `minute_close`, and never relabel either as live execution,
  an exchange fill, queue acknowledgement, or guaranteed real fill.

- The active dual-RTX-5090 `tw_minute` long/short contract copies the ordinary
  TW day-trade trading rules and changes only decision frequency: L1 gross
  exposure `1.0` directly from raw signed scores (no de-meaning),
  `max_volume_participation: 0.5`, normal fees/tax, zero extra slippage, and no
  outside-cash, per-order-notional, or per-name ceiling.
  Exact-session short eligibility and prior-session official short capacity
  remain correctness constraints, not stress-test overlays.

- Apply eligibility, volume, turnover, demonstrated depth, legal-price, and
  whole-lot constraints independently to each day-trade symbol.  Retain every
  remaining executable quantity exactly as constrained; do not rebalance,
  scale, prune, or fail the portfolio flat to preserve a requested aggregate
  long/short ratio.  Continuous training/backtest, the exact integer oracle,
  minute execution, and live paper execution must use this same independent
  per-symbol contract.

- Day-trade historical minute-curve repair is local-first. Search the ordered
  receipt-backed one-minute KBar chunks, materialized `trade_date` research
  partitions, and retained local minute cache before starting any Shioaji
  request. Delegate only unresolved `symbol + session_date` gaps to the
  canonical `download_shioaji_tw_minute_kbars.py` collector, and receipt both
  pre-fetch and post-fetch source coverage. Never re-download a locally covered
  pair or substitute an interpolated/fabricated price.

- Every day-trade ledger replay/backfill must rebuild and validate one-minute
  curves before promotion. Each completed session and active paper mode must
  contain exactly 270 right-labelled points from 09:01 through 13:30; a daily
  endpoint-only curve or a stale `minute_curve_receipt.json` is not promotable.
  A still-open current session may be deferred, but the weekday post-close
  minute-curve maintenance event must finalize it once the historical source is
  available. Preserve accepted entry/exit fills and the 13:30 ledger endpoint.
  Historical 09:01 marks must value held quantities at the completed 09:01
  source Close with the same net-liquidation fee accounting as later minutes,
  not merely preserve an entry-fee-only mark and delay its price change to
  09:02. Revaluation requires an explicit receipt and unchanged fill hash;
  missing 09:01 source prices fail closed. Disclose carried last trades and
  never use linear interpolation. The dashboard's full-minute mode must retain
  every available minute, including multi-month selections, and rebase each
  series at its first selected valid mark without modifying absolute equity.

- TW day-trade discipline (user correction, 2026-09-10): same-day liquidation
  is mandatory whenever executable evidence permits it. The staged live
  `day_trade_strict_intraday` contract keeps unfilled entry targets working,
  latches triggered stops, and observes actual closing auction prints through
  the 13:33 delayed auction (bounded receipt deadline 13:35). Missing quotes,
  ordinary capacity exhaustion, and program errors are unresolved exits, not
  authorization for general margin carry. Only evidenced adverse limit-lock
  with zero counterparty depth can be an exceptional paper carry; it remains
  critical and reduction-only at the next opportunity, never a new model's
  retained inventory or proof of broker financing approval. Do not retrofit
  today's failed fills or change old replay/training semantics silently.
  See `docs/DAY_TRADE_INTRADAY_DISCIPLINE_2026-09-10.md` for activation status.

- Dashboard strategy names come from the active mode descriptor (`label`), not
  the stable ledger `market` key. Use the shared TW presentation and detail
  components; never hard-code model aliases in individual tables or rename
  persisted IDs to fix display text. Stock-futures badges are read-only current
  TAIFEX catalog membership with an observed date, not point-in-time signal
  eligibility, liquidity, or permission to trade. Missing, invalid, or expired
  catalog evidence is unknown, never false. Keep this enrichment out of model
  inputs and execution, and never call a broker/network API per displayed row.
  See `docs/public_dashboards_architecture.md` for component and producer gates.

- Historical low-rank long/short control: `portfolio_mode: long_short`.
- Keep `trading.long_only: false` when the model is intended to do long/short.
- For legacy forced-L1/logits controls, portfolio direction and sizing use raw score direction followed by L1 normalization. This does not override the learned-cash stock policy or a product-specific action ABI:
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
  the model's unscaled base exposure: `1.0` for forced-normalization modes and
  at most `1.0` for explicit L1-ball residual-cash modes. The multiplier
  produces separate `leverage_*` plots with turnover and fees recomputed from
  scaled weights. It remains part of the checkpoint configuration fingerprint
  so resumed reporting is reproducible.
- Rank-only loss can over-concentrate positions. If using rank objectives, keep turnover/concentration/backtest regularization in mind.
- If the user switches back to only-long behavior, change both the model direction mode and the loss/backtest direction assumptions deliberately and report the change.

## Canonical Tensor Backtest And Loss

### Full-stock-context single-stock-futures day trade

- `tw_stock_futures_day_trade_0845_minute` is the current daily-decision baseline:
  prior-completed stock features at 08:45, first completed 08:46 futures minute
  execution, 13:20 passive limit, 13:24 market replacement, final 13:30 deadline.
  A 13:24 order change first consumes the 13:25 right-labelled bar. The minute
  tape is executor-only; capacity is per physical contract and per minute.
  Residual contracts remain explicit execution failures, never stock margin
  conversions or fabricated 13:45/daily-close fills. Source coverage, hashes,
  whole quantities, costs, and clock belong to the checkpoint contract.
- Its explicit historical v2 variant appends daily-regime/open/close/capacity
  channels to the unchanged v1 event tape. Early daily capacity uses prior
  session volume; post-cutoff capacity and unfilled-exit failure use actual
  minute facts. Missing post-cutoff sources must not become zero-return days.
- The legacy `tw_stock_futures_day_trade_0900` research control keeps the
  complete ordered Taiwan cash-stock feature panel and may use data complete
  through `t-1` plus the dedicated observed
  `log(OPEN[t] / CLOSE[t-1])` channel at 09:00. No session-`t` high, low, close,
  full-session volume, futures return, or post-09:00 execution result may enter
  the model input. `tw_stock_futures_day_trade` remains the legacy 08:45
  OPEN-to-CLOSE control and must keep the current stock OPEN gap disabled.
- Map at most one nearby single-stock future to each `date + underlying` using
  source `tenor_rank=1`, preceding-session contract existence, nearest tenor,
  then preceding-session volume/open interest and stable product codes.  Never
  use session-t volume or return to choose the product.
- Apply the causally known futures mask before portfolio normalization.  Apply
  current observed entry/CLOSE/positive-volume execution gates afterwards and
  set each failed request to zero independently; never redistribute its unused
  weight or capacity to another name. A stock without a known future has zero
  effective output and cannot trade. The 09:00 mode must not use the execution
  mask in its policy normalization or model inputs.
- The legacy 09:00 research ledger
  computes the selected physical future's immutable TAIFEX day-session
  OPEN-to-CLOSE return. The OPEN is stamped 08:45 and precedes the 09:00 model
  decision, so this is a counterfactual label, not a causally executable fill
  or live-performance claim. Daily OPEN/CLOSE/positive-volume evidence still
  fails closed independently per contract. The receipt-backed first public
  trade strictly after 09:00 remains an opt-in sidecar source, not the default.
- The canonical continuous ledger uses two fixed per-contract commissions,
  date-versioned per-side futures transaction tax rounded to whole TWD, and a
  capacity ceiling based on the selected contract's completed preceding-
  session volume valued at the actual declared entry price. Cash-stock fees,
  price limits, and short inventory do not govern this futures execution path.
- The 09:00 model and its 08:45 minute successor use raw long/short scores with no cross-sectional
  de-meaning, divides by the active causal-futures count, and projects onto the
  unit L1 ball. Therefore gross exposure is learned in `[0,1]` and residual
  capital may remain cash; do not add top-K, fixed long/short ratios, an outside-
  cash heuristic, or forced full investment. Current execution failures remain
  unfilled and their unused risk is not redistributed.
- The legacy 09:00 entry/return source is the immutable aligned daily table
  `data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet` with
  `tw_stock_futures_day_trade_0900_entry_source: daily_session_open_proxy`.
  `post_0900_trade_sidecar` may opt into
  `data_tw_futures/taifex_stock_futures_0900_v1/entry_0900.parquet` only when
  its complete manifest and source hashes exist. A new source, mapping rule,
  capital, fee, tax, feature timing, exposure transform, or accounting basis
  requires a new artifact root and must not resume incompatible checkpoints.

The project goal is to keep train, validation, test, and inference return logic consistent and tensor-friendly.

Rules:

- `tw_cash` is now a two-auction carrying contract: model actions have shape
  `[T,2,S]` in `[OPEN,CLOSE]` order and each channel is a signed post-event
  target. Both auctions share one daily volume/turnover/borrow-capacity budget,
  their gross fees are charged separately, and their cash flows are netted into
  exactly one T+2 claim after the close.
- `tw_overnight` is a strict one-session cohort contract with actions
  `[T,3,S]`: resolved exit-at-open fraction, signed open entry, and signed close
  entry. New entries cannot exit on their entry date; the prior cohort must be
  completely closed by the next close or the account enters absorbing default.
  Same-direction rollover is still an explicit gross exit plus re-entry. The
  raw model head parameterizes one signed daily entry direction plus a
  differentiable OPEN/CLOSE timing split; the resolved entry channels must
  therefore have the same sign per symbol. Never net opposing same-day entry
  directions, because that would silently create a day trade.
- Both carrying-mode heads use completed daily features through `t-1` plus the
  dedicated final-row `log(open[t] / close[t-1])` channel. This is the user's
  explicit observed-open contract, matching the existing open-aware day-trade
  approximation. No session-`t` high/low/close/full-day-volume field may enter
  either head; quote availability, limits, fill masks, and close[t] remain
  executor-only. Opening short eligibility has its own
  `can_short_open_open_mask` and must never be inferred from a closing mask.
- Phase-action preparation must mask non-tradable symbols before activation/L1
  allocation, and model plus simulator normalization must share the same
  `PORTFOLIO_L1_EPS`. For P3, OPEN/CLOSE entry allocations share one direction;
  the model adapter fail-closes an impossible opposite-sign pair to zero and
  the direct ledger APIs reject it.
- The user's 2026-09-09 fixed 13:25 overnight research follow-up explicitly
  authorizes `data.overnight_1325_missing_price_policy: same_session_close`.
  Prefer a verified 13:25 price; when it is absent, use that same session's
  finite official daily close as the decision-price proxy. Preserve 23 prior
  daily features, the close-entry/next-open exit clock, and the full requested
  history. Record a per-symbol/session fallback mask, counts, and the
  same-close look-ahead caveat in the prepared data/checkpoint/report contract.
  Missing both prices remains unavailable; corrupt hashes/receipts/clocks
  remain errors. Use the v2 close-fallback artifact root and never resume v1
  checkpoints. This research exception does not authorize fabricated live
  quotes/fills or change the strict default (`reject`).
- Apply permissions, volume, turnover, and borrow capacity to the requested
  signed endpoint before splitting it into buy/sell/short-cover/short-open
  legs. A cross-zero transition must first reduce the existing side; blocked
  reductions cannot be bypassed by opening the opposite side. Phase volume and
  short-capacity inputs are prior-close-valued share equivalents and are
  revalued at each auction; a disabled short-capacity ceiling is represented
  by positive infinity and must remain infinity-safe.
- Public phase turnover and executed-leg weights use the day's opening NAV as
  their common audit denominator. Execution histories are recorded after the
  auction but before cash-dividend entitlement; recurrent `final_weights` and
  `final_due_weights` use the post-entitlement closing NAV.
- Voluntary short-cover funding must use actual account-wide maintenance
  release. Affordability along a producer-first cover ray can be non-monotone:
  a small cover may be unaffordable while a larger cover unlocks collateral.
  The continuous hot path therefore evaluates the finite analytic breakpoints
  and affine roots and takes the global maximum feasible point in O(S); never
  replace it with a prefix bisection. The integer oracle must examine exact
  rational lot endpoints in descending order, including minimum commissions
  and rounding. Mandatory covers bypass voluntary funding and may create a
  later absorbing T+2 default.
- The P2/P3 dual-session CUDA executor keeps the eager ledger as its semantic
  oracle and processes fixed 32-session blocks by reusing one
  `fullgraph=True, dynamic=False` daily kernel with CUDA graphs disabled.
  Recurrent cash, T+2 queues, due cohorts, collateral, alive state, and
  autograd remain connected across all 32 calls. Batches shorter than 32 rows
  reuse the same daily compiled kernel, including the batch-16 overnight
  baseline; they must pass the strict compiled-loss probe without changing
  optimizer cadence. Only a non-aligned tail after a complete block is
  eager. Do not replace this with a fully unrolled 32-day FX graph: its
  Inductor scheduling/codegen cost is pathological, while measured two-day and
  four-day kernels were slower or much more expensive to compile.
- Phase modes currently fail closed for scalar rank/direction auxiliary losses,
  `activation_l1` with the shared consumer/model activation field,
  per-fold explainability, and the single-target live signal preview. Do not
  flatten `[B,P,S]` into `[B,S]`; P3 due-exit fractions are not signed exposure.
- On the measured RTX 5070 Ti at `T=32, S=2735` (median of seven steady
  repetitions), P2 cash forward was `57.54ms` versus `1302.96ms` eager and
  forward+backward was `160.05ms` versus `2826.10ms` eager. P3 overnight
  forward was `54.73ms` versus `1940.13ms` eager and forward+backward was
  `192.64ms` versus `4062.07ms` eager. Maximum action-gradient differences
  were `5.37e-7` and `4.85e-7`; every run used one unique graph with zero graph
  breaks and fallbacks.
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
- The exact all-futures account must reserve cash for blocked carried positions
  before opening new targets.  If independently rounded group targets exceed
  account equity, contract all requested exposures by one common radial scale
  and reselect whole-contract baskets; do not add top-K, long/short quotas, or
  freed-cash redistribution.  A funding default is valid only when the proven
  minimum-cash capacity-feasible basket also exceeds equity.  Fixed fees and
  reversals can make the discrete frontier non-monotone, so the search must
  retain that minimum-cash fallback and recheck the final account inequality.
- For exact futures, live account equity is the sole owner of hard denomination
  rounding.  The model may encode denomination/cost features, but a fixed-
  reference-capital model projection followed by executor rounding is forbidden
  for new contracts because it double-quantizes actions.  Preserve it only for
  legacy replay and record the projection owner in the checkpoint manifest.
- A futures funding-recovery penalty must have zero slope inside its funded
  feasible set. An everywhere-smooth softplus penalty can reverse profitable
  fee-adjusted utility gradients even with ample cash. Penalize positive
  funding excess only; retain the exact forward funding gate, absorbing ruin,
  and recovery beyond the constraint. Additional risk aversion belongs in an
  explicit objective, not an invisible backward-only cash preference. See
  `test/test_futures_solvent_funding_gradient.py` for two-sided counterexamples.
- When an exact recurrent log-utility config selects full-trajectory optimizer
  cadence, every chronological batch in one epoch must see the same parameters.
  Weight decomposable batch losses by their valid-row share, detach documented
  recurrent state at batch boundaries, then clip, update AdamW, and advance the
  step scheduler exactly once.  Any non-finite batch invalidates the complete
  trajectory; never continue with cleared partial gradients.  Epoch-zero
  pretrained validation must also reject defaults or non-positive/non-finite
  ending equity even if its ruin-clamped scalar loss is finite.  Chunked exact
  validation must retain default and default-reason histories.  A rejected
  transfer may preserve its backbone only by resetting a proven trainable final
  action head to an exactly flat cash portfolio, recording that fallback, and
  requiring later checkpoints to beat the exact zero-loss cash baseline; never
  weaken the solvency guard or accept the clamped dead-account loss.
- If compiled loss hits CUDA Graph overwritten-output errors, only fall back the loss wrapper to eager tensor loss; do not disable model `torch.compile` globally.

The user's 2026-09-30 futures policy retains unfilled day-trade exits into the
next session without an invented penalty or automatic account default. Keep
the actual signed whole contracts, daily mark-to-market, ordinary executed-side
fees/tax, and applicable overnight margin. The same distinction applies to an
unfilled margin-risk reduction: the dated-margin account v6 retains its residual
and re-evaluates the next session's margin; gradient contract v10 adds no loss
merely for an unfilled order. Non-positive economic equity remains absorbing.
Contract expiry, physical delivery, missing continuation evidence and dated
position limits are separate obligations. A flat-only minute source cannot be
silently treated as verified overnight data. The existing physical minute carry
executor owns that continuation; all-futures minute attachment still needs its
own complete physical-history evidence before admission. See
[the dated implementation report](../tw_futures_unfilled_carry_2026-09-30.md) and
[TAIFEX's day-trade margin procedure](https://www.taifex.com.tw/chinese/11/attach/111年7月25日台期結字第1110002405號.pdf).

The user's 2026-09-16 stock day-trade v7 assumption makes only the final 13:30
liquidation capacity-unbounded.  Preserve source-derived 50% capacity for entry
and the ordinary 13:20/13:24 exit path; at the terminal step, close every
remaining deliverable share at the official close by setting the reducer's
quantity to the account's exact residual, not an arbitrary large capacity.
This is reduction-only: it cannot open or reverse inventory.  Missing terminal
price, source gaps, halts, and undelivered corporate-action shares remain
fail-closed or legally locked; do not fabricate a quote or call this observed
auction liquidity.  The flag
`tw_day_trade_terminal_liquidation_unlimited_capacity` changes executed
quantities, loss and the checkpoint fingerprint, so the active v7 config uses a
fresh artifact root while the named v6 config preserves capacity-limited
residual-to-margin replay.

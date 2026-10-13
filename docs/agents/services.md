# Discord and TAIFEX service contracts

Use the section for the service being changed. Operational acceptance requires fresh runtime evidence; historical service observations do not establish current readiness.

## Discord Service Reliability Contract

- Treat Discord/Gateway liveness and private artifact maintenance as separate
  health domains. A stale or failed formal-history job must remain visible as
  degraded maintenance, but it must not disconnect commands or stop the
  independent day-trade execution engine.
- Keep `pre_signal_timeout_seconds` for bounded data activation and
  `formal_history_timeout_seconds` for full fold inference. Never shorten the
  latter to the former; measure full-universe inference before changing either.
- Artifact-maintenance attempts must persist a structured status receipt across
  restarts, use bounded exponential retry, suppress duplicate channel alerts,
  and become `ready` only after the requested history or signal artifact exists.
- A stale manual `/signal_now` must persist `waiting_source` keyed by target
  date/config/user before returning. It must not run a stale preview or call an
  activation command as though activation downloaded official data. The
  canonical source-event/acceptance pipeline owns downloads; after it reports
  fresh, resume one serialized inference and DM, including after a bot restart.
  Defer interactive jobs during the protected opening window.
- Closed-market TW day-trade inference has a separate completed-session gate.
  Once an official TWSE/TPEx close receipt is accepted, atomically rebuild the
  canonical stock panel and public feature table through that close and value
  manual signals from the official close. Do not require the next session's
  eligibility, MIS opening quote, or opening activation for this calculation;
  those remain mandatory only for executable 09:00 paper orders. Never relabel
  an after-close calculation as a fill at that already-completed close.
- Live TW panel/cache reads share the canonical producer's
  `tw-public-refresh.lock` across the source read. An exclusive producer means
  retryable `waiting_source`, not a completed mixed reference/entitlement
  generation. Once that owner releases, strict SHA/count/dependency checks still
  reject real corruption. Postclose cache telemetry is owned by the current bot
  run and may recover after the provisional window only from an accepted
  official-close signal or a matching deployment's complete existing cache;
  this does not establish formal-history or broker-fill acceptance. See
  [2026-10-08 source-coherence repair](../tw_corporate_action_generation_repair_2026-10-08.md).
- Set the outer systemd watchdog above measured legitimate event-loop stalls.
  Keep the tighter opening path protected by its progress-aware hot/cold
  watchdog so relaxing the outer process watchdog does not relax 09:00 recovery.
- Bound and rotate traceback logs. Service acceptance requires the Gateway to
  be connected, command sync to succeed or be explicitly deferred, all enabled
  TW modes to acknowledge the engine revision with zero lag, and logs
  since the current restart to contain no watchdog or fatal error.

## TW Day-Trade Engine Responsiveness

- The ledger/execution loop owns watchdog liveness. Historical-query progress
  may publish status, but must not keep a stalled execution loop falsely alive.
- Missed-open source recovery runs off the ledger loop, publishes each resolved
  symbol durably, and releases each mode from its own source dependencies rather
  than waiting for every mode. Prune only provably zero whole-lot targets using
  frozen session sizing NAV; carried inventory and unknown prices remain in scope.
- Opening acquisition contract v4 distinguishes positive same-session opens,
  fresh official zero-volume/no-first-print evidence, and unknown source failures.
  The existing input quorum may count the first two as resolved observations;
  response rows alone, stale timestamps, or a ready flag cannot prove readiness.
  Observed absence uses the existing zero-gap feature sentinel, never a fabricated
  price. Positive-open completeness remains a separate, truthfully incomplete metric.
  Preserve accepted opens and retry unknown inputs through the shared source owner.
  A mode's retry must not starve source repair between actual inference attempts.
  After publishing the immutable original model weights once, an unpriced target
  remains a per-symbol unsized `waiting_source` order, not a discarded cash target
  or a reason to rerun the whole model. Freeze its original session NAV; size only
  after real same-session price/limits and causal non-trial evidence arrive, and
  cancel unopened targets when the 13:20 exit window begins. Preserve eligibility,
  short permissions, lot/tick rules, causal quotes, and the original fill policy.
  Exchange timestamps and local receipt timestamps remain distinct; a fresh
  delivery of yesterday's row is not today's opening. See the initial
  [2026-10-07 repair](../tw_opening_source_recovery_2026-10-07.md) and its
  [2026-10-08 quorum/pending-target correction](../tw_opening_latency_repair_2026-10-08.md).
- The live TW opening source owner starts every configured provider over the
  current official TWSE/TPEx regular stock/ETF trading universe concurrently.
  A dated ISIN registry plus official halt/status receipt is refreshed during
  preopen preparation (final-arm rechecks receipts older than 15 minutes), not
  fetched under the opening/model lock. Model coverage uses its intersection
  with that official universe; historical `alive_mask`, positive volume and a
  fixed count are not current market-membership authorities. Keep the complete
  checkpoint symbol axis, report official symbols absent from each model, and
  never backdate a midday registry observation into a historical signal.
  The owner accepts incremental batches and persists verified
  inputs before releasing the existing quorum. Slow/failed providers cannot
  cancel winners or hold the model mutex; later attempts reuse accepted facts
  and retry only unresolved deltas. Preserve conflicting raw observations and
  the complete requested universe. Shared IPC progress is cumulative, and native
  callback parsing/network waits stay outside the stock-cache and execution locks.
  The pre-open transport probes are also independent: one proven responding
  source may warm the model, but cannot bypass actual 09:00 source coverage,
  eligibility or execution evidence. Daily `*.acquisition.jsonl`, the scheduled
  opening-attempt log and engine entry telemetry have separate measurement
  boundaries; a source quorum, paper fill and broker fill are different proofs.
  Native Shioaji stock Snapshot wall-clock timestamps are localized to Taipei
  before conversion to UTC milliseconds; do not shift their clock by eight hours
  or silently rewrite historical receipts. The shared quote worker remains in
  the existing simulation engine process/session, not another login. See the
  [multi-source implementation and bounded measurements](../tw_opening_latency_repair_2026-10-08.md#多源平行開盤取價2026-10-08-續作).
- A current-session Snapshot book may supply explicitly labelled indicative
  valuation only. Unknown `simtrade` remains unknown; these fields must not
  supply execution prices, volume, bracket triggers or non-trial evidence.
  Missing liquidation-side prices remain stale, including locked-limit books.
- Subscription rotation must respect the provider's subscription cap. Faster
  reconciliation is not proof of complete executable-price coverage. Preserve
  missed opening SLO and missing minute-curve evidence after service recovery.
- Evidence and regression commands: [2026-10-01 web-service repair](../tw_day_trade_web_service_repair_2026-10-01.md).

## TAIFEX Live Strategy Dashboard Contract

- Before the multi-worker runner freezes its one shared held-option subscription
  snapshot, run the strategy engine once in settlement-only mode under the same
  engine lock.  Cash-settle an expired weekly cycle and any expired
  `put_call_parity_tx` package only from the official TAIFEX final-settlement
  parquet, then snapshot the remaining current contracts.  Missing official
  settlement, a residual futures hedge, or malformed leg metadata must fail
  closed.  Do not require an expired contract to remain in the next trading
  date's Shioaji catalogue; that ordering deadlocks settlement before startup.
- Successful expiry settlement is a continuous-roll boundary, not a terminal
  strategy state.  On the first complete fresh Bid/Ask set, alive weekly
  strategies select the nearest listed weekly expiry strictly after the current
  TAIFEX trading date, while `put_call_parity_tx` scans the current TX future's
  same-expiry monthly TXO pairs.  Repeat this data-driven transition at every
  expiry; never revive an absorbing-ruin ledger or fabricate a roll from stale
  depth.
- Active-cycle restart compatibility may reconstruct the old shared ATM
  Call/Put pair only for pre-v5 execution states.  A current flat, pending,
  futures-only, ruined, or independent parity ledger must remain exactly flat
  across restart.  Contract v9 repairs only the evidenced v8 corruption shape:
  exactly one active-cycle Call plus one Put whose net deltas are both zero in
  the immutable ideal trade ledger; ledger-backed positions remain untouched.
- The Shioaji TAIFEX strategy engine runs only on market-data worker 0. At the
  start of every multi-worker capture, read the persisted non-zero option
  positions exactly once, pass the same code snapshot to every worker, and pin
  each required complete Call/Put pair ahead of the ordinary ATM selection so
  it remains within worker 0's 100-contract cap. Missing, expired, incomplete,
  or over-capacity held pairs must fail startup closed.
- Construct the engine from the option contracts actually assigned to worker 0,
  then assert that every persisted held option code is subscribed before
  reconciliation or strategy steps. Generic fleet coverage on another worker is
  not strategy valuation coverage.
- Keep dashboard feed coverage, held-leg subscription/book coverage, fresh
  executable strategy valuation, recent explicitly labelled `CARRIED`
  valuation, and unavailable valuation as separate metrics. Never show generic
  `100/100` callbacks as proof that all strategy curves are currently valued.

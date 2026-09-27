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
- Set the outer systemd watchdog above measured legitimate event-loop stalls.
  Keep the tighter opening path protected by its progress-aware hot/cold
  watchdog so relaxing the outer process watchdog does not relax 09:00 recovery.
- Bound and rotate traceback logs. Service acceptance requires the Gateway to
  be connected, command sync to succeed or be explicitly deferred, the three
  intended TW modes to acknowledge the engine revision with zero lag, and logs
  since the current restart to contain no watchdog or fatal error.

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


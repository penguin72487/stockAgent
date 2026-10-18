# Discord and TAIFEX service contracts

Use the section for the service being changed. Operational acceptance requires fresh runtime evidence; historical service observations do not establish current readiness.

## Shioaji SDK maintenance

- `scripts/maintain_shioaji_sdk.py` queries official PyPI stable releases,
  rejects yanked/pre-release/incompatible wheels, verifies SHA256 and validates
  the native Python API plus focused quote, volume, scheduling and execution
  tests in an isolated target. It never logs in or submits an order and never
  upgrades unrelated CUDA/data packages to make a candidate pass.
- Install with `source scripts/runtime_env.sh` followed by
  `run_fintech_python scripts/install_shioaji_sdk_maintenance.py`. The canonical
  timer checks hourly at `:40` and runs daily at 05:30 Asia/Taipei. Promotion is
  allowed only 05:15–06:30, after the night session and before preopen; a late
  boot catch-up checks versions without restarting trading owners.
- Native SDKs are process-local. Stop reviewed existing SDK owners before
  replacing the package, then restore only previously active services/timers.
  Coordinate with the existing paper-service control lease; an unmanaged SDK
  process defers promotion rather than being killed. Installation failures use
  the preserved exact previous wheel; a durable journal and `ExecStopPost`
  recover interrupted replacement. Preserve the original failure receipts.
- Inspect `artifacts/operations/shioaji_sdk/status.json`, candidate acceptance,
  transaction and private logs. Installed version, mapped native inode,
  successful owner restart, real login/quotes and market readiness are distinct
  proofs. Use existing source/owner receipts and preopen gates for the latter;
  offline compatibility tests never imply a broker fill or complete live data.

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
- The postclose path notification is a latency hint, not the only recovery
  mechanism: the canonical receipt-only retry controller also checks completed
  cache dependencies without importing Torch. Latest-close and full-history
  owners retain separate runtime/runway budgets; do not place the two-hour
  history restriction around their shared controller. Probe a source lock with
  a shared lease so a legitimate reader is not mistaken for an exclusive
  producer; job locks still suppress duplicate workers. Only complete canonical
  summary/weights readback across a stable generation acknowledges a wakeup.
  A dispatch, deferred exit, partial artifact or metadata-only plan is not data
  acceptance. Unchanged plans avoid repeated YAML/Arrow/model imports while
  preserving per-file and membership invalidation; actual signal consumers
  retain their full validation. Scoped maintenance installation must not restart
  Gateway, source, quote or execution owners. Measurements and native evidence
  are in [the running-service report](../running_service_optimization_2026-10-13.md).
- Set the outer systemd watchdog above measured legitimate event-loop stalls.
  Keep the tighter opening path protected by its progress-aware hot/cold
  watchdog so relaxing the outer process watchdog does not relax 09:00 recovery.
  During `preopen_catch_up`, respect the existing source timeout only while
  that resolved shared command has a real live child or a recently verified
  blocked writer-lock wait. Its deadline must not slide, and waiting is not
  data readiness/model progress. Quote/model/input stages retain their original
  watchdog. Evidence and bounded owner checks are in the
  [2026-10-14 source-watchdog repair](../running_service_optimization_2026-10-13.md#0900-開盤現場入口權限與來源-watchdog-修復).
- Bound and rotate traceback logs. Service acceptance requires the Gateway to
  be connected, command sync to succeed or be explicitly deferred, all enabled
  TW modes to acknowledge the engine revision with zero lag, and logs
  since the current restart to contain no watchdog or fatal error.
- Scheduled latency-critical input work uses one session/config-fenced job per
  mode across bounded scheduler polls. A slow source preparation cannot prevent
  another mode's existing retry; cancelling a `to_thread` await is not proof
  its builder stopped, so do not replace a running job with a duplicate. Discard
  stale results after completion and retain canonical inference revalidation.
  Reports/chat run in their original delivery order after immutable publication,
  outside the input-retry loop. See the measured
  [opening dispatch and execution boundaries](../tw_opening_latency_stages_2026-10-14.md).
- GPU residency follows the same verified session/preparation window as model
  warmup, not a separate hard-coded 07:00 retention rule. Successful serialized
  idle eviction invalidates the process-hot flag; it does not change signals,
  inputs, precision or permanent-failure classification. A completed warmup
  receipt is historical evidence, not current residency. The measured penguin
  ten-minute lead and actual cold-owner readback are scoped in
  [preparation and release evidence](../completed_session_inference_cache_2026-10-13.md#盤前暖機與釋放窗口).
- A committed opening signal is not proof that a restarted Discord process has
  warm model caches. After the protected opening window, the existing no-quote,
  `write=False` warmup may recover cold caches even when the engine already
  acknowledged today's signals; active opening attempts and interactive jobs
  keep priority. The qualified reload tool defaults to plan-only; `--apply`
  performs a cutover and `--discord-only` preserves the execution owner. Require
  current model/entrypoint identities, same-run complete warmup, engine ACK and
  preserved ledger prefixes; a connected Gateway alone is insufficient. See
  [the 2026-10-15 continuation](../tw_opening_latency_stages_2026-10-14.md#discord-與完整特徵共享續修2026-10-15).
- Explicit `--discord-only --code-admission-only` recovery separates fresh
  loaded-code/Gateway/ACK acceptance from GPU readiness. Only complete same-run
  per-mode `LoadedImplementationChanged` rejections may recover a degraded
  preflight; unknown errors, active work, paper/ledger and opening guards remain.
  Report `accepted_code_admission_only`, never warm-ready by implication, and
  leave warmup/eviction to the native preparation clock. Plan/apply and final
  post-prefix checks use the existing bounded coherent ACK reader, not retained
  snapshots past heartbeat TTL. Journal proof binds one invocation and filters
  startup, fatal and warm-ready separately before taking bounded results; a
  successful marker or fatal must not disappear behind ordinary progress logs.
- Inference code identity follows the transitive repository imports of the
  canonical inference/cache entrypoints, including lazy imports, package
  initializers and literal dynamic imports. Do not fingerprint entire data or
  model directories: unrelated news development is not a model-admission gate.
  Cache parsed syntax/closure only with consumed-file generations and import
  candidate membership checks; adding/deleting an imported module must still
  invalidate admission. Real dependency changes retain the fresh-owner rule;
  never clear/rebind an old owner's admission to bypass it. See the
  [scoped-dependency repair and native acceptance](../tw_opening_latency_stages_2026-10-14.md#discord-相依範圍修正與今日訊號恢復2026-10-16).
- Healthy FinMind opening retries have a bounded wake clock inside the shared
  fan-in, not behind slow MIS/broker completion or the scheduler's next pass.
  Keep shared snapshot single-flight, request-start cadence, accepted facts,
  unresolved deltas and provider error/quota backoff. The native daily opening
  audit is an independent no-network, journal-only observer: freeze the enabled
  mode roster before open, retain real owner preparation context and original
  publication/full-target commit clocks, and never overwrite the first deadline
  assessment with replay or late repair. Missing scope/evidence remains unverified;
  a successful audit process is not a passed trading SLO. Scoped installation
  uses `scripts/install_discord_bot_service.sh --opening-audit-only` without
  restarting market owners. See [daily evidence and measured limits](../tw_opening_latency_stages_2026-10-14.md#11秒日常驗收與獨立-finmind-喚醒2026-10-16).
- FinMind connection reuse and decoded full-market views are source-owned, not
  worker-thread/model-owned. Fence date/open gate/PID/credential rotation and
  preserve original receipt clocks on reuse. Retain full-payload coverage and
  missing reasons independently of shrinking model gaps; consumer-delta events
  only establish lower bounds. Measure response headers/body, validation,
  source-local first valid/full coverage and durable model input separately;
  cached data, missing rows and HTTP success are not fresh market-wide evidence.
  See [source forensics and paired measurements](../tw_opening_latency_stages_2026-10-14.md#finmind-首筆全量測速與449檔根因2026-10-16).

## TW Day-Trade Engine Responsiveness

- The ledger/execution loop owns watchdog liveness. Historical-query progress
  may publish status, but must not keep a stalled execution loop falsely alive.
- Opening execution timing observes changed target quantities only after the
  canonical state/projections are durably committed. Original publication,
  registration, first paper fill and full-target completion are separate clocks.
  Replay event times, cash-only modes, cancelled unsized targets and completion
  found after restore cannot manufacture live opening SLO success. Observer I/O
  failure stays visible without blocking liquidation or changing the ledger.
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
  Full-market snapshot endpoints are single-flight across model deltas. FinMind
  uses a bounded one-second request-start cadence only in the first eleven
  opening seconds, then ten seconds; its approximate data refresh interval is
  not a client rate limit or a fill SLA. Valid old-date responses remain a data
  wait, while transport/permission/quota backoff and all source-clock checks
  remain intact. Read-only 08:30 acceptance and its final revision fence use
  shared leases; escalate and revalidate only for real source mutations. See
  [the 11-second investigation](../tw_opening_latency_stages_2026-10-14.md#finmind-開盤補查與-11-秒目標2026-10-16).
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
  settlement or malformed leg metadata must fail closed. A residual futures
  hedge fails closed unless the explicit paper-recovery policy below is enabled.
  Do not require an expired contract to remain in the next trading
  date's Shioaji catalogue; that ordering deadlocks settlement before startup.
- Contract v13 adds user-authorized `--recover-expired-cycle-futures` (runner
  `SHIOAJI_TAIFEX_RECOVER_EXPIRED_CYCLE_FUTURES=true`, default false). Preserve a
  verified backup before enabling it. Settle expired options from the official
  receipt, freeze futures identities from the matching original-cycle status,
  and preserve nonzero futures until fresh non-trial same-contract five-level
  depth can completely reduce that strategy to zero. Missing/mismatched contract
  evidence remains blocked; insufficient depth retries without inventing a fill.
  No new cycle, calibration, parity entry or risk expansion runs while reduction
  is pending. Paper repair disables broker orders persistently, records its
  actual receipt/execution times and explicit repair reason, and resumes through
  the existing worker 0 after reduction. It is not a backdated 9/18 or broker fill.
- Dashboard v9 ages valuation evidence against the observer clock. Preserve the
  original source trading date; `observed_trading_date` describes today's session,
  not a backdated mark. Historical cache invalidation uses marks/backfill versions
  plus the shared strategy/capital projection inputs, not every status heartbeat.
  Reject persisted caches from an old dashboard schema. If a changed source is
  more than two minutes ahead of a cached curve, rebuild the actual projection
  instead of amplifying days-old data through stacked stale-while-refresh caches.
  The UI distinguishes current strategy health from a curve still catching up.
  The native prewarmer keeps the default 1d generation current on mark changes
  and stops with the server. TAIFEX public history does not add a second SWR
  layer; the native projection is its single refresh owner.
  History cache v2 freezes one source timestamp upper bound across native and
  canonical readers; marks appended during a rebuild belong to the next query.
  The anchor includes verified replay generations as well as the live ledger.
  Ranges share a projection only when their effective source window and bucket
  geometry are identical; on a short history, quarter/year/all need one full
  scan, not three. Warmup 503/timeout observations remain visible and are retried
  without a page reload. They are not accepted as data completion or zero latency.
  The read-only physical projection may use role-local `base_delta_v2`: reuse a
  fully verified v1 inode, keep an immutable base plus a small delta, and compact
  at the declared source-tail bound. Changed sources still require complete
  retained-prefix SHA and generation fences; preserve the canonical parser and
  query/accounting/schema. Pinned payloads and the original source lease span
  the query. Never rewrite closed sources merely to create a fresh deployment
  receipt: native reader adoption and a natural new-source update are separate
  proofs. See [measured rollout](../penguin_service_storage_layout_2026-10-15.md#1016-0520-續修taifex-曲線投影重用固定歷史與小型增量).
- Verify paper repair with `scripts/verify_taifex_web_recovery.py`: supply the
  verified `--backup-dir`, original `--capture-id`, output directory and local /
  public `--url` values. It checks preserved ledger prefixes, official source
  hashes/intrinsic values, raw captured five-level fills, every strategy's current
  curve endpoint, read-only HTTP behavior and native responsive interactions.
  With `--observe-minute-refresh`, the browser must observe the actual scheduler
  advancing its endpoint without reloads. The public warmup probe injects one
  browser-only 503 and verifies waiting/retry/real-API recovery without changing
  production sources or caches; real HTTP failures remain separate in receipts.
  Browser `cpu-2d` verification is explicit and does not prove GPU/WebGL readiness.
  The [2026-10-13 recovery acceptance](../../artifacts/operations/taifex_web_repair_20261013/accepted-release/acceptance.json)
  verified 28 original futures reductions and 108 option settlements against
  preserved ledger prefixes and original price evidence, plus all seven ranges
  on local/public APIs and laptop/tablet/phone browser widths. It did not replay
  the historical interruption or establish actual broker fills.
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

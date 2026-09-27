# Taiwan public source and execution-rule contracts

Use for Taiwan official downloaders, parsers, price precision, listing lifecycles, public rules and source/cache receipts. Find the affected source or contract in this file; unrelated parser histories need not be loaded. Version numbers identify the documented contract generation, not a ban on a verified versioned successor.

## TW Public Execution-Rule Contract

### Taiwan price precision and dated tick contract

- A quoted trade, Bid/Ask, or limit order is constrained by the exchange's
  **product + trading date + venue/currency + order type** tick schedule.
  `stockagent/data/tw_price_rules.py` owns supported regular stock/ETF
  outright grids in their quote currencies and the 2026-07-06 stock-futures
  change (1 TWD ticks extend from below 1,000 to below 2,500). Never use the
  cash-stock grid for stock futures on or after that date. Query dated Shioaji
  `tick_rule`/`tick_bands()`
  for live FOP contract metadata when available and reconcile it with official
  notices; current metadata cannot rewrite historical quotes.
- Market labels from the broker are not a point-in-time listing-status proof:
  `tpex` minute KBars include emerging issues. The emerging-stock tick was
  fixed at TWD 0.01 before 2020-03-23 and follows six regular-stock bands
  thereafter. The TPEx officially lists 2743 on 2020-03-09 and 6716 on
  2020-03-27; older minute observations for these two are emerging. Admit
  other historical transitions only after checking their dated official
  listing notices, not by guessing from first observed data or retroactively
  relabeling current records. Changes to the price-rule contract and eligible
  market universe are distinct; independently audit the strategy's admission
  of emerging securities before declaring a historical execution realistic.
  `scripts/audit_tw_emerging_stock_admission.py --strict` reports two 6716
  pre-listing source minute bars with both feature and label eligibility, but
  zero effective training rows after the dated admission mask. The schema-5
  minute loader must remove excluded rows from model masks, session state and
  fitted normalization moments; the schema-4 day-trade minute tape must block
  those symbols before any executable price/volume is assigned. Keep
  `stockagent/data/tw_listing_admission.py`, both loader paths, research
  backtests, cache keys, dataset fingerprints, checkpoint and artifact contracts
  aligned. A new listing date needs official evidence and a new contract
  version; never rewrite raw source or silently resume old checkpoints.
- Index-future outright prices, option *premiums*, calendar-spread prices,
  block trades, and option strikes use different grids or semantics. In
  particular TXO ordinary premium has five bands, while 2019-05-27 onward
  TXO block trades use 0.1; TEO premiums change on 2025-12-08; stock futures
  change on 2026-07-06. Do not apply a 2026 Shioaji Contract V2 snapshot to
  past non-equity futures to assert that historical ticks were unchanged.
- A tick is **not** the number of significant digits for every field. Source
  OHLC/trade/Bid/Ask prices may be checked against their grid. VWAP, average
  price, official daily/final settlement, adjusted price/index, corporate-action
  cash and stock entitlements, FX rates, percentages, ratios, features, fees,
  NAV, and contract-adjustment cash values retain source/calculation precision.
  Never round those fields to a quote tick, two decimal places, or a generic
  display format. Preserve original source bytes/strings and units; derive
  order prices only with directional quantization, then validate the result.
- Each new market-price source must declare field semantics and applicable
  historical rule dates. A TWSE foreign-currency ETF counter (sixth symbol
  character K/M/S/C) uses the ETF numerical tick in its own quote currency;
  the suffix alone does not identify CNY versus USD. Other unsupported
  foreign-currency quotes,
  block auctions, spread orders, unknown effective dates, and contract
  adjustments must be marked unverified and fail closed for executable-price
  claims. Do not infer a rule from current product specifications or from a
  source column named `price`. Run `scripts/audit_tw_price_precision.py` to
  record source SHA, per-field eligible and off-grid counts; review exceptions
  against dated exchange evidence before changing producer data or training
  labels. See `docs/tw_price_precision_tick_contract.md`.
- TWSE odd-lot quoted prices use the ordinary-market price tick according to
  its published trading-system guide, so source odd-lot bid/ask/close may
  share the stock price-grid audit. This does not make its auction clock,
  volume unit, or available execution capacity identical to board lots.
- `scripts/audit_tw_price_precision.py --full` audits registered cash OHLC,
  stock/ETF futures, TX/MTX/TMF, TXO and selected minute/HFT/historical
  broker prices. `scripts/audit_tw_capture_price_grid.py` audits raw capture
  price columns against the worker-selected files without relying on the
  mutable `top_200.csv`. Its receipt explicitly cannot prove the historical
  universe SHA when that CSV is no longer available. Keep price-grid validity,
  source provenance, capture completeness, PIT, and executable fills separate.
- A malformed official daily date is a source defect even when reported date
  coverage appears complete. TWSE `2014-12-;1 / 8070` was repaired only by a
  verified 2014-12-31 official re-fetch plus exact-symbol replacement; the
  dependent symbol parquet and public feature table were rebuilt. Daily mode
  must stop on such rows, and repair mode must keep an unmatched malformed row
  visible rather than guessing its date. An old feature or panel cache cannot
  inherit the new source receipt merely because its file still exists.

- Never put a data-dependent `torch._assert_async` in a compiled model,
  settlement, loss, or backtest CUDA hot path. A failed device assertion poisons
  the CUDA context and causes every DDP/NCCL rank to abort. Validate static input
  contracts at eager host boundaries. For runtime market facts, use deterministic
  tensor semantics: an impossible mandatory exit is an absorbing account default;
  a day-trade round trip with a missing close leg or invalid valuation never opens.
- `tw_cash.short_maintenance_ratio` limits collateral released by a cover. If an
  account was already below maintenance before a partial cover, release zero
  collateral and retain/reassign the existing pools; do not assert and do not
  invent an unconfigured same-day margin-call cure. A complete cover still
  releases all collateral. This v8 return/default change is part of
  `CANONICAL_BACKTEST_CONTRACT_VERSION`.
- Corporate-action receipt `requested_start_year` is the latest incremental
  downloader request boundary, not the historical archive boundary. Panel
  completeness and TW cash avoidance must use cumulative
  `coverage_start_year` (falling back only for legacy receipts), and that
  interpretation must remain part of the panel cache contract key.
- Preserve two distinct receipt-derived corporate-action masks. Avoid mode uses
  the full interval from the last close where both long and short positions can
  be flattened through the last cum-right close for every official action.
  Exact mode uses the unresolved-only interval plus the issuer entitlement and
  payment ledger. Never reconstruct avoid mode by OR-ing a one-row cash-yield
  event into the unresolved mask; the event row can be halted or limit-blocked.
  The effective mode-specific mask is part of the checkpoint data fingerprint.

- Use `downloader/download_tw_official_data.py` as the canonical TWSE/TPEx-first
  data-layer entry point. Its modes are `rebuild` (staged from-zero replacement),
  `repair` (audit local historical coverage and fetch missing/suspicious dates),
  and `daily` (verified-baseline incremental update with a recent correction
  overlap). Daily mode must fail when no completed rebuild/repair baseline exists.
- Canonical TW OHLCV starts at 2000-01-01. TWSE/TPEx rows always win the same
  `date + symbol` key. The approved `yahoo_fallback` may fill only otherwise
  missing stock/ETF OHLCV rows from 2000 onward; it must pass through
  `scripts/build_tw_yahoo_fallback_archive.py`, preserve row-level `data_source`,
  preserve `adjustment_source` when Yahoo supplies only a missing return factor,
  and retain content receipts. Official OHLCV must remain untouched when only its
  reference/change factor is missing. Yahoo must not fill public features, execution
  rules, valuation, margin, institutional, lifecycle, or corporate-action data.
- Source retention and the model horizon are separate contracts. The current
  receipt-certified archive keeps 2000+ rows, but 80 official delisted-company
  histories ending no later than 2004-04-28 are terminal-unavailable from Yahoo
  and precede complete free official coverage. Do not fabricate or silently
  ignore them. Until a provenance-backed backfill is obtained, TW training
  configs must use `data.panel_start_date: 2005-01-01`, which yields the first
  session 2005-01-03 and 100% official-delisted coverage inside the model
  horizon. Audit retained source rows against the full verified TAIEX calendar
  while auditing panel/universe/walk-forward semantics against the clipped model
  calendar. A newly proven backfill may reopen the earlier horizon only after a
  strict re-audit.
- Changing `data.panel_start_date` changes the experiment and checkpoint
  identity. With a 2005 first year, validation years 2023/2024/2025 are folds
  18/19/20, not 23/24/25. Keep `walk_forward.expected_first_year`, runner fold
  selection, explainability, benchmarks, and cached panel keys aligned.
- Fresh TW Yahoo fallback downloads default to one worker and a 1.5-second global
  request interval. If the bare chart endpoint is rate-limited, the installed
  yfinance session is the same-provider fallback. Persist
  `stockagent.yahoo_requested_start`; repair must re-query from 2000 when that
  historical coverage receipt is absent. Reusing one fixed rebuild stage must
  skip already-completed atomic symbol files.
- Direct Yahoo downloader invocations use the same provider-named host-global
  limiter and default to 10 requests/second when `--request-interval` is omitted;
  the staged TW bootstrap deliberately overrides that policy with the slower
  1.5-second interval above. A repair symbol universe must be the stable union of
  live discovery, cached/repository manifests, locally tracked parquet, and the
  canonical TWSE/TPEx delisted-company parquets. Do not let a successful current
  listing query erase historical or delisted symbols.
- A Yahoo TW source parquet is eligible for the lower-priority archive only when
  its schema metadata says `stockagent.source=yahoo`,
  `stockagent.asset_class=tw_stocks`, its `yahoo_requested_start` reaches the
  requested archive start, and `yahoo_checked_through` reaches the requested end.
  The archive must account for every manifest symbol as either a verified source
  file or a terminal unavailable result, write full per-file size/SHA-256 and
  coverage metadata to the adjacent `.inputs.json`, and receipt that manifest in
  the summary. Downstream symbol build/audit must fail closed on a missing,
  stale, or tampered input receipt chain.
- When Yahoo fallback is enabled, build the receipt-verified
  `tw_transfer_adjustment_reference.parquet` stage after the per-symbol Yahoo
  source update and before `build_tw_yahoo_fallback_archive.py`. Its official
  requests use the `tw_public` provider-global limiter; an omitted interval keeps
  the project default of 10 requests/second. The archive may fill
  `source_factor` only when a reference `date + symbol` matches that canonical
  Yahoo source's first retained row and the original factor is null. Every
  reference key must be applied exactly once; incomplete coverage, unresolved
  rows, stale input/output receipts, non-first-row matches, duplicate matches,
  unmatched keys, or an empty historical candidate set fail closed. Receipt
  both the reference parquet and its summary in the Yahoo archive input manifest
  and reconcile required-candidate, reference, and applied counts. Do not run
  this stage with `--ohlcv-fallback none`.
- Historical downloader success is coverage-based, not inferred from receiving
  any rows. Persist confirmed no-data weekdays separately from request failures;
  any unresolved date failure must produce a nonzero exit. A failed rebuild must
  leave production parquet files untouched.
- Historical rebuild request outcomes are append-only per-date JSONL journals
  under `state/journals`, with successful parsed rows retained in fingerprinted
  `state/partials` parquet while coverage is incomplete. Default `--resume` must
  reuse validated partial dates, confirmed-empty journal events, and reparsable
  atomic raw receipts, then request only unresolved/failed/corrupt/suspicious
  dates. Partial success remains nonzero and must never weaken the final
  coverage/audit/promotion gate. `--no-resume` is the explicit fresh-refetch
  escape hatch. Increment `HISTORICAL_PARSER_CONTRACT_VERSION` whenever raw
  historical response parsing semantics change so an old parsed partial is not
  silently reused under a new parser. A parser bump may reparse a prior
  content-addressed `raw_failures` receipt across old cache keys only when its
  append-only event is `failed/network/HTTP 200`, its official URL and date
  match, its path stays under the dataset failure directory, and filename,
  byte counts, full raw/body SHA-256, nonempty current parse, and current schema
  all validate. Any mismatch remains unresolved for a network retry. On POSIX,
  each historical dataset also
  holds a nonblocking process lock at `state/locks/<dataset>.lock` for the whole
  mutation; a second writer to the same dataset/stage must fail immediately.
- Canonical historical runs must use the receipt-verified monthly TAIEX archive
  as their actual-session calendar. Keep `twse_daily_ohlcv` and
  `twse_market_index` at parser contract v7. A measured rebuild disproved the
  old assumption that a selected MI_INDEX table title was authoritative: TWSE
  returned bodies whose table title was rewritten to the requested day while
  top-level `payload.date`, `params.date`, and all data rows belonged to another
  session. This
  affected 18 real-session OHLCV receipts and 7 TAIEX closes, not only holidays.
  v7 requires the selected table title, top-level `payload.date`, and any
  supplied `params.date` to declare the requested date. Never bind a retitled
  body to `request_date`, and
  never reuse v6 partials as current parsed data. During a resumed rebuild,
  reparse raw receipts into the v7 partial, reuse only receipts that pass every
  date check, and network-refetch the rejected dates; stale receipt bytes
  cannot be repaired by reparsing. For legacy TPEx rows with the exact official
  sentinel
  `open=high=low=0` and a positive close, preserve the raw row; the derived
  symbol parquet may create a flat bar at that official close only when it also
  records `ohlc_normalization=official_close_flat_bar`. Yahoo still must not
  overwrite that official row.
- Canonical Yahoo OHLCV fallback must be bounded by receipt-verified official
  security lifecycle episodes. After a terminal TWSE/TPEx delisting, discard
  fallback rows until a later current-company, new-listing, or official daily
  listing observation verifies a new episode; reset the derived adjustment
  index to 10 at that episode boundary. A same-day or next-official-session
  same-symbol venue migration is a nonterminal continuation, including official
  `櫃轉市`, and must not be truncated or reset. Persist lifecycle evidence on
  retained rows and reconcile every lifecycle-filtered fallback row in the
  symbol-build summary. Never attach a later Yahoo reuse of a code (for example
  post-2007 `9801`) to the old delisted company without official relisting
  evidence.
- A TPEx row's `次日參考價` prices only the immediately following
  receipt-verified official session. When today's exact adjustment reference is
  otherwise unavailable, the builder may use
  `close_today / previous_session.next_reference` only when the previous symbol
  row is that exact preceding session, both rows remain in the same lifecycle
  episode, and the reference and close are positive finite values. Record the
  previous session/date/reference as row provenance and reconcile the candidate
  and applied counts in the build summary. Never use today's `次日參考價` as
  today's reference, and never bridge a missing session or lifecycle boundary.
- Keep the TPEx daily parser at contract v12 for the verified layout sequence:
  width 27 on 2003-08-01--2004-01-30, width 26 on
  2004-02-02--2004-10-27, width 18 on 2004-10-28--2004-11-24,
  width 19 on 2004-11-25--2006-12-29, and width 17 in the legacy JSON `html`
  on 2007-01-02--2007-06-29. Preserve every available quote/statistics field.
  Bind archive dates only from a labeled compact ROC date or the exact damaged
  Oracle header cell (`width=71`, `colspan=5`, `rowspan=2`,
  `class=table-body-right`, one `<tt>` date), and require it to match the request.
- TPEx v12 may preserve permanently damaged security names only with
  `_name_decode_status=official_receipt_name_bytes_unrecoverable`. Receipt-level
  replacement-byte evidence applies that status to every name row in the
  receipt, because CP950 can re-pair `EF BF BD` into plausible CJK text. It may recover
  only the three evidenced CP950 change renderings for `除權`, `除息`, and
  `除權息`, recording `_change_decode_status`; any unknown damaged symbol,
  numeric, or change token fails closed. A `均價=註` row is valid only under the
  exact zero-price/zero-change/zero-volume/zero-amount/zero-trades gate.
- A TPEx row with all four prices zero but positive, internally consistent
  volume/amount/average is an official unpriceable observation, not a usable
  OHLC bar. Preserve it in raw public data; never substitute average for close.
  A valid Yahoo bar may fill the canonical key with
  `fallback_reason=official_ohlcv_unusable`. The symbol-build summary must
  reconcile `official_unusable_ohlcv_rows` as
  `fallback_replaced_unusable_official_rows + unfilled_unusable_official_rows`.
- Keep `tpex_margin_balance` at parser contract v8. v8 adds the exact
  2004-10-19 onward 16-cell generation, preserving the real trailing blank
  note cell, plus its narrowly styled standalone ROC-date header. Do not accept
  a 15-cell row: preserving the blank `<td>` is what distinguishes an empty
  note from a genuinely missing column. The former v7 331-session
  `2007-06-01`--`2008-09-29` source-gap annotation was disproved by direct
  official re-query on 2026-09-16; those sessions were backfilled. Historical
  `raw_empty` receipts remain investigation evidence, never a substitute for
  current nonempty official data or proof of coverage. Re-audit the actual
  official response and TAIEX calendar rather than reinstating a date-range
  exception.
- Keep `tpex_daily_valuation` at parser contract v7. The 2004--2006 archive
  declares its requested day as a labeled ROC date such as
  `交易日期:94年08月08日`; bind that exact date and still fail on missing or
  mismatched labels.
- Public HTTP throttling is provider-named and host-global across stockAgent
  threads and subprocesses. For an upstream without a documented numeric limit,
  the project default is 10 requests/second; this is a client-side safety policy,
  not an official allowance. Explicit slower intervals are valid, and 403/429,
  `Retry-After`, or transient-server backoff must defer the shared provider
  schedule. Treat TWSE's HTTP 307 `FOR SECURITY REASONS` page as a provider-wide
  WAF signal. For `twse_market_index`, cross-check primary `rwd` failures and
  structured weekday empties through the official
  `exchangeReport/MI_INDEX?type=IND` route; reliable `IND` coverage starts on
  2009-01-05. The four other TWSE histories use official legacy fallbacks:
  `MI_INDEX?type=ALLBUT0999` for daily OHLCV, `exchangeReport/BWIBBU_d` for
  valuation, `fund/T86` for institutions, and `exchangeReport/MI_MARGN` for
  margin. Retry a semantically stale fallback with a unique `_` cachebuster.
  Under the v7 MI_INDEX contract, validate the selected target-table title,
  top-level `payload.date`, and any supplied `params.date` together; a retitled
  table never overrides a stale top-level or parameter date.
  A live WAF recovery recheck used the provider-global
  `--request-interval 1.0` (one request/second); retain that slower measured
  setting for WAF-sensitive repair rather than treating 10 req/s as guaranteed.
  The official findings and URLs live in
  `docs/tw_public_download_resume_and_rate_limits.md`.
- For an HTTP 200 body that fails semantic parsing, discard only the current
  thread-local HTTP session before route/retry/cachebuster handling. Do not add
  another provider-global defer or sleep for that semantic retry: its next HTTP
  call still passes through the 10 req/s default limiter, while WAF, 429,
  transport, and `Retry-After` backoff remain provider-global inside `_http_get`.
- Strict-calendar state finalization must prune malformed or stale non-session
  `failed_dates` keys, plus failures resolved by verified data/empty receipts.
  Keep actual unresolved session failures. Record the last prune count/examples
  and cumulative `pruned_failed_dates_total` so cleanup can make coverage
  complete without erasing the audit trail.
- Canonical TW stock/ETF symbol files live under `data_tw_public/stocks`. Do not
  run the former in-place official-to-Yahoo mutation script; the canonical
  lower-priority archive merge is the only approved Yahoo fallback path.

- Backfill official lifecycle/short-sale announcements with
  `run_fintech_python downloader/download_tw_short_sale_restrictions.py --output-dir data_tw_public --start-year 1995 --end-year <year>`, then rebuild
  `data_tw_public/features/tw_public_stock_daily.parquet` with
  `scripts/build_tw_public_training_features.py`. The downloader is strict by
  default: it writes a completeness report and refuses to replace data after an
  incomplete archive request unless `--allow-partial` is explicitly chosen.

- `data.use_tw_public_features` controls model inputs; `data.use_tw_public_rules`
  independently controls execution masks. A rules-only TW baseline must not append
  `twpub_*` features or read their parquet columns.
- When `use_tw_public_rules: true`, the configured public parquet is required;
  fail before panel construction instead of silently training without market rules.
- `can_sell_mask` means an existing long may be sold. `can_short_open_mask` means a
  new/increased short may be opened. Do not merge them: a short-sale ban must not
  prevent an investor from selling an owned long position.
- Ordinary non-tradability, missing data, zero volume, halts, and price-limit blocks
  freeze the affected position. Only an explicit official permanent-exit event sets
  `force_exit_mask` and settles a position (with the applicable buy/sell fee).
- An official permanent-exit date may follow a quote-less suspension. Place its
  `force_exit_mask` on the final finite positive close of that security episode,
  while blocking the delisted interval from the official event date onward. Never
  ask exact-share execution to fabricate a termination-day price or reuse a prior
  incarnation's close after a genuine relisting boundary.
- Exact-share holdings reports must reconcile claims, risky marks, collateral,
  and NAV at one accounting instant. A cash-dividend receivable earned after the
  row's execution mark belongs to end-of-row queues, not the execution-time cash
  row; preserve separate execution-time queue totals instead of mixing them.
- Do not treat every venue-level delisting as a terminal asset exit. TPEx rows
  identified by the official TWSE new-listing feed as `櫃轉市` continue under the
  same symbol and must not fabricate a sale/fee. Likewise, if a symbol resumes on
  the immediately following panel session, treat it as a same-symbol market or
  corporate transition; a later relisting after a real gap remains a new
  incarnation and the old position exits first.
- `_twpub_official_traded` may contain disjoint archive snapshots. Infer a missing
  trading session only between nearby observations (currently at most seven
  calendar days); never convert a long source-coverage gap into a multi-year halt.
  Long halts come from explicit official halt/resume events.
- Fund notices often write an ETF code only in parentheses. Keep broker-tradable
  ETF beneficiary certificates in the short-ban/terminal parser while continuing
  to exclude warrants, ETNs, preferreds, and ordinary debt instruments.
- Delisting and short-cover announcements are point-in-time state transitions.
  Process them chronologically by market and symbol, and allow a later cancellation
  to remove only a still-pending delisting/cover obligation while preserving an
  explicitly continuing short-open ban.
- Relative cover rules use their stated anchor. The usual rule is ten exchange
  sessions before delisting; notices that say six sessions before stop-transfer use
  the stop-transfer start date instead.
- Panel cache v2 writes immutable generations under a writer lock and atomically
  commits metadata last. Readers retry the complete snapshot if a concurrent writer
  reclaims the generation sampled by an earlier metadata read.
- Panel cache validation fingerprints every source byte (including the external
  rule parquet), so a same-size replacement with preserved timestamps cannot
  silently reuse stale execution masks.


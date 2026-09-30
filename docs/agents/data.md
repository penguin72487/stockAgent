# Panel, feature and crypto data contracts

Read the relevant section for panel backends, feature availability, or crypto acquisition. Preserve data semantics and point-in-time gates. Hardware timings and provider observations require current verification before reuse.

## Data Panel Backend Contract

The full US Yahoo universe is a distinct data-processing regime. Benchmark it
with the actual full parquet directory instead of small synthetic subsets when
choosing defaults.

Rules:

- `scripts/benchmark_data_backends.py` is the reproducible scanner/benchmark for
  data-processing hotspots. Its active optimization scope is PyArrow plus Polars
  Lazy/Streaming; do not add compatibility/reference paths outside those backends.
- Do not add DuckDB or cuDF back to the panel/runtime benchmark set unless a new
  request explicitly reopens those candidates.
- For full US daily parquet (`16811` files, `S≈16811`) with
  `tradable_mode: tradable`, runtime `build_panel(... panel_backend="auto")`
  should select Polars Lazy when available, then PyArrow. Explicit
  `panel_backend="polars"` is an alias for `polars_lazy`; explicit
  `panel_backend="polars_streaming"` is available for measurement but is not the
  current auto default.
- US Yahoo parquet files under `us_stocks` must keep `Trading_Volume` for
  trainable stock-like assets. If old `_DL`/delisted/archive parquet files are
  missing that column, treat them as schema-broken data: repair should normalize
  `_DL` records back to the base Yahoo symbol and remove unusable delisted
  schema-mismatch files instead of letting panel build fail on the first file.
- For the US Yahoo universe, do not remove a symbol only because it is currently
  delisted; historical delisted common stocks/ADRs/ETFs are needed to reduce
  survivorship bias. Do exclude security types outside the normal broker-tradable
  stock/ETF/ADR universe, such as warrants, rights, units, preferred/depositary
  preferreds, and exchange-listed notes/debt instruments.
- Foreground full-US PyArrow+Polars benchmark after narrowing the scope:
  `panel_build` measured Polars Lazy `69.36s`, Polars Streaming `86.55s`, and
  PyArrow `195.85s` on recheck. PyArrow checksum was
  `8707711790994.017`; Polars Lazy differed by about `913` in feature checksum
  on very large OHLC anomaly-derived ratios, while returns and masks matched in
  spot checks. Use `panel_backend="pyarrow"` when bitwise checksum parity is more
  important than panel-build speed.
- Wide full-US daily weight parquet output (`512 x 16811`) should prefer Polars
  Streaming sink among active native backends: repeat-5 foreground benchmark
  measured Polars Streaming `1.81s`, Polars Lazy `2.13s`, and PyArrow `4.02s`.
- Feature-prep proxy benchmarks favor PyArrow because they use direct Arrow to
  NumPy arrays: PyArrow `29.58s` on recheck, Polars Lazy `206.41s`, and Polars
  Streaming `296.95s`. For the current many-small-files US layout, PyArrow's
  single-pass full-table read is faster than adding a per-file schema projection
  pass.

## Crypto Downloader Baseline

The active crypto downloader baseline is one-minute bars.

Rules:

- Yahoo `crypto`, OKX perpetual, Bybit perpetual, and Binance USD-M perpetual
  downloaders should treat completed `1m` candles as the canonical intraday
  candle source.
- Keep canonical `daily`, `1m`, and actual `tick` data in separate directories.
  Never relabel candles or snapshots as tick events.
- Do not silently merge legacy daily or 15-minute crypto parquet rows with new
  one-minute rows. Rebuild/migrate into the dedicated `1m` path instead.
- Derive daily crypto bars from validated one-minute rows only when a stronger
  provider-native daily adjustment contract is unavailable, and persist minute
  row counts plus expected-row coverage.
- Provider statistics that are natively slower than one minute (for example
  five-minute OI/ratio series) must retain their native grain and may be
  causally carried/aligned to a one-minute decision grid; never fabricate
  one-minute observations.
- For a newly listed crypto contract in tail-only mode, anchor the lookback to
  the latest **completed** candle at or before the requested end, not the
  requested date's possibly future UTC 23:59. Preserve the official listing
  lower bound and do not mark an empty or not-yet-listed source complete. A
  Taiwan early-morning run exposed this failure simultaneously in OKX, Bybit,
  and Binance; bound forward request windows by the same completed-candle end.
- Keep stock and FX Yahoo downloads on daily bars unless the user explicitly changes those markets too.

## Feature Engineering Guardrails

Explainability indicated suspicious dependence on raw price level and raw liquidity.

Rules:

- Avoid feeding raw OHLC price levels directly when the goal is cross-stock generalization.
- Prefer log returns, relative price ratios, rolling normalization, and engineered K-line/volume features.
- If changing feature schema, update cache/versioning so stale panel caches are not reused.
- Keep `return_1d`, tradable masks, TW limit guards, and benchmark construction aligned with the canonical backtest.
- TW public snapshot-only families remain forbidden in strict/live model inputs. Never
  add them to a strict/live `data.feature_include`, model categorical-feature list,
  explainability selection, or replacement model schema: `twpub_monthly_revenue_*`,
  `twpub_cumulative_revenue_yoy`, `twpub_financial_*`, `twpub_insider_*`,
  `twpub_borrow_*`, `twpub_sbl_*`, `twpub_short_sale_available_*`,
  `twpub_tdcc_*`, and `twpub_company_*`. Their raw/source columns may remain
  available solely for provenance, auditing, and future data-quality research;
  they must not influence strict/live training, validation, test, inference, or
  feature importance. The user's 2026-09-19 all-features request explicitly
  permits these observed-date values only in the separate
  `tw_public_preopen_all_observed_research_2014_v3.yaml` research ABI. There,
  keep capture dates and missingness channels, never backdate a current snapshot
  to an earlier year, shift unproven capture-session snapshots to the next
  exchange session at a 09:00 decision, and never claim historical PIT or
  executable performance.
- The 2026-09-18 broad-history request authorizes a **separate** research-only
  table/config for obtainable current-revision macro values and historical
  MOPS XBRL facts mapped by labelled theoretical release dates. That earlier
  request did not repeal the snapshot-only denylist; the later v3 exception
  above is limited to its separate research ABI. In that v1 wide ABI, use
  `twpub_xbrl_*` columns from quarter archives, not recent
  `twpub_financial_*` OpenAPI snapshots.
  Keep the canonical strict/live feature table and checkpoint ABI unchanged.
  The wide config removes log/asinh inputs, retains ratios, carries only
  released state features, and adds per-feature availability channels before
  NaN-to-zero. Retired TIFRS concepts expire after 400 calendar days without
  a new observation; do not carry their last value across taxonomy eras.
  Event flags such as attention/disposal must never forward-fill;
  source coverage is a separate channel and pre-capture zeros are unknown.
  Current revisions and estimated dates are research approximations, not
  verified historical PIT values. See `docs/tw_public_wide_research_2014.md`.
- The 2026-09-28 request additionally permits documented historical release
  schedules in the separate `tw_preopen_release_schedule_research_v3` ABI.
  Preserve the audited strict base, original-value missingness, source receipts,
  late-upload lower bounds and explicit estimated-time/current-revision flags.
  Invalid state observations are NULL barriers, not permission to carry the
  previous value through an invalid report. This does not authorize retrocasting
  current snapshots or enabling these channels in strict/live configs. See
  `docs/tw_release_schedule_research_2026-09-28.md` and
  `scripts/build_tw_release_schedule_dataset.py` for the opt-in contract.
- The later 2026-09-28 request caps **additional safety delay** at one calendar
  day in `tw_preopen_release_schedule_research_v4_delay1d` and allows genuine
  missing-only alternate observations in a separate cross-source research ABI.
  Distinguish reporting period, estimated/known publication, safety delay and
  exchange-closed wait; a known upload replaces a guessed later deadline.
  Compare original economic keys/units before daily alignment, preserve finite
  primary observations and explicit official-conflict masks, and retain invalid
  state NULL barriers through sparse overlays. Do not equate FinMind monthly
  `create_time` with an issuer announcement (its 2026-04-21 bootstrap is not
  historical release evidence). Provider quota, unimplemented semantic mapping,
  and licensing restrictions are not proof that a value is unavailable online.
  The measured gate and unresolved scope live in
  `docs/tw_feature_gap_repair_2026-09-28.md`, not strict/live eligibility rules.
- In the separate 2026-09-28 semantic-repair research ABI, NULL is not a source
  corruption verdict. Diagnose cadence, applicability, missing report items,
  formula domains, warmup and publication history separately. TDCC was monthly
  before May 2015 and weekly thereafter; use the dated carry rule, not today's
  weekly expiry across all history. NMI's retrospective 2014 observations were
  first released on 2015-02-02 and cannot be used earlier. Preserve the one-day
  safety delay. An event absence may become zero only after reconciling the
  complete venue/day report, and financial identities require exact issuer,
  period, units, validated overlap and the latest operand's availability clock.
  Optional missing accounts are not automatically zero. Rebuild a new dataset
  and pass `scripts/verify_tw_feature_semantics.py` before refreshing the
  worklist; keep original sources, explicit conflict masks and strict/live
  configs unchanged. Evidence: `docs/tw_feature_gap_repair_2026-09-28.md`.

### TW Phase-Aware Current-Open Feature Contract

- The only model-visible session-`t` opening quote is the opt-in normalized
  feature `next_session_open_gap_logret`. Panel row `r` stores
  `log(open[r+1] / close[r])`; because phase-aware Taiwan modes keep their
  execution feature lag at one, the final row of a target-`t` window exposes
  exactly `log(open[t] / close[t-1])`.
- Do not replace this with a raw nominal opening-price feature. The normalized
  gap is invariant to stock price scale and does not let the model identify a
  symbol merely from its price level.
- This feature is opt-in and legal only with the phase-aware
  `tw_day_trade`, `tw_cash`, and `tw_overnight` execution modes.
  Empty/wildcard panel feature selection must retain the close-complete schema
  and must not silently include a next-session quote.
- No session-`t` high, low, close, or volume may enter the model window used to
  submit its opening order. Same-session full-day volume is future information.
  Execution participation limits must continue to use completed session `t-1`
  share volume as their causal proxy and keep that volume in the execution
  layer, not in the model input.
- Historical 98-feature close-complete checkpoints remain a separate schema.
  Use a new artifact root and retrain for the 99-feature open-aware model.

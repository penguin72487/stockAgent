# Walk-forward and benchmark contracts

Use for split ownership, model horizons, benchmark returns and stitched reporting. Resolve the requested experiment before applying its product-specific horizon or capital.

## Intentional Walk-Forward Semantics

- Every equity or ETF benchmark is a total-return benchmark.  Daily research
  uses the source's adjusted close so cash dividends, ETF distributions,
  stock dividends, splits, and reverse splits are represented once.  Live raw
  Bid/Ask benchmarks instead multiply held units by each official ex-date
  `previous_close / reference_price` factor before executable liquidation
  costs.  Never add a separate dividend cash flow to an already adjusted
  close or adjusted-unit series.  Futures, FX, and crypto have no equity cash
  distribution; preserve their product-specific price/roll return contract.

- The `tw_index_futures_day` market comparator is not the strategy's daily
  open-to-close TX target. It is a gross, fully collateralized 1x long TX
  front-month buy-and-hold series. Preserve every monthly contract in futures
  data contract v2. When the front month changes, roll at the preceding session
  close and compute the new row from that new contract's own preceding close;
  never book the old/new calendar-spread price gap as return. Persist the fold
  roll path in `futures_benchmark_audit.npz`.
- The active exact TX/MTX/TMF integer audit capital is TWD 100,000,000. Keep it
  in the mode-specific checkpoint contract and use a fresh artifact root when
  changing it. Do not force a minimum one-contract trade to hide granularity;
  that would violate the model's requested risk exposure.
- The exact integer executor must apply price-dependent PnL, tax, and slippage
  arithmetic only to products with nonzero selected contract quantities.
  Missing prices on an unavailable zero-quantity product are inert; an active
  product with a non-finite open/close is a hard data error. Keep exact and
  continuous-surrogate test metrics explicitly labeled in console output so
  the surrogate cannot be mistaken for the canonical annual report.

- TW day-trade point-in-time eligibility is absent before 2014 in the verified
  public data and first becomes executable on 2014-01-06 (sell-first coverage
  begins 2014-06-30). A `tw_day_trade` experiment must not start its training
  panel in 2005 or project today's eligibility backward. The trainer's execution-
  coverage preflight must reject any train/validation split with zero executable
  round trips because its canonical loss is constant and all model gradients are
  exactly zero. The active public day-trade config therefore starts in 2014.
- A TW day-trade strategy row executes open[t] to close[t]. Its configured
  symbol benchmark is buy-and-hold over the same wall-clock session, using the
  panel's adjusted-close forward label shifted one row: close[t-1] to close[t].
  Do not replace it with a cross-sectional mean of intraday returns or use the
  unshifted close[t] to close[t+1] label. The active public benchmark is 2330.

- When `walk_forward.require_future_test_year: false`, the final experimental fold
  deliberately reuses its validation window as its test window. Keep that overlap;
  label it as latest-year experimentation rather than unbiased model selection.
- Every mode uses canonical `lookback_context: panel_history`: a new split owns
  its first eligible target and may read earlier, already-observed panel rows to
  construct the causal feature window. A lookback of 32 therefore does not drop
  the first 31 target dates of every validation/test year. It never imports
  earlier returns, portfolio state, or future information.
- `split_only` remains a legacy reproduction option for historical checkpoints,
  not an active training default. Panel-slab densification and dynamic-symbol
  upper bounds must preserve the panel-history target endpoints; do not recompute
  a split-only start and silently drop the first `lookback-1` owned targets.
- For stitched deployment tests, the next model owns its new-year first target
  under panel history; the preceding model stops immediately before that target.
- Incremental Taiwan stock-account reports replay only the earliest available
  consecutive completed-fold interval. A pre-existing later fold remains in
  independent reset-state diagnostics but is deferred from stitched curves until
  the intervening folds complete; never jump missing sessions or insert synthetic
  flat rows to age T+2 claims. `walkforward_deployment_coverage.json` records the
  report contract, stitched and deferred fold IDs. Session gaps within a fold or
  between adjacent folds remain hard errors. This is report-only and does not
  invalidate compatible model/optimizer checkpoints.
- Derive stitched deployment prefix ownership from the authoritative
  `CrossSectionalDataset.valid_indices` of the full fold test. Do not recompute
  mode-specific row eligibility inside the report helper: in particular, the
  crypto dataset removes a globally unfinished trailing research period, and
  that removed row must not reappear in deployment artifacts.

# TAIFEX futures training data map

This repository treats a futures observation as useful only when four facts are
known: what market state it measures, when it became public, whether it was
tradable at that time, and which immutable source receipt can reproduce it.
More columns without those facts increase leakage rather than information.

## Canonical layers

| Layer | Canonical path | Measured coverage | Training role |
|---|---|---|---|
| All listed futures daily contracts | `data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet` | 1998-07 onward | OHLC, settlement, volume, open interest, term structure and contract lifecycle |
| TX day-session contracts | `data_tw_index_futures/day_session_contracts.parquet` | 1998-07 onward | Receipt-verified TAIFEX session calendar and index-futures state |
| TXO full daily chains | `data_tw_index_options_daily/*_full_chain.parquet` | 2001-12 onward | Volatility surface, skew, parity and option-implied state |
| Official recent TX/TXO ticks | `data_tw_index_derivatives_ticks` | Rolling free official window | Trade tape and official recent microstructure audit |
| Shioaji futures/options history | `data_tw_shioaji_history` | API availability boundary | One-minute KBars, historical trades and prospective live books |
| Public positioning history | `data_taifex_public_history/normalized` | Source-dependent | Put/call ratios, institutional positions and large-trader concentration |
| TAIFEX OpenAPI vintages | `data_taifex_public_history/receipts/openapi` | From first local capture | Current risk, margin, limit, contract, Delta and market-statistics state |
| Global CFTC positioning | `data_cftc_legacy` plus `data_openBB/compact/cftc/cot` | 1986 onward, source/report dependent | Cross-market commercial, managed-money and concentration state |

Coverage dates above are operational observations and must be remeasured from
the manifests before a training release is promoted.

## Public-history collector

Run the resumable collector manually with:

```bash
scripts/run_taifex_public_history.sh
```

The registered systemd timer runs it after the official daily refresh. It
retains deterministic compressed raw bytes, one hash receipt and one normalized
Parquet shard per request, then rebuilds merged Parquet only from verified
shards. The packed-data catalog prevents publication while this writer is
active.

The official all-futures daily refresh also updates the TAIFEX product-code
master and rebuilds the contract-v4 fixed-slot training panel. Unknown product
codes fail closed; adjusted contracts without a historical contract-unit notice
remain archived but are excluded from fixed-fee research instead of receiving
an invented multiplier.

The static CFTC pre-2000 gap can be reverified without touching the active
OpenBB scheduler with:

```bash
scripts/run_cftc_legacy_pre2000.sh
```

The normalized datasets are:

- `put_call_ratio.parquet`: official TXO market-wide put/call volume and open
  interest ratios, starting 2001-12-24.
- `institutional_futures.parquet`: dealer, investment-trust and foreign futures
  trades plus open interest by product.
- `institutional_options.parquet`: the equivalent option totals by product.
- `institutional_calls_puts.parquet`: option positioning separated into calls
  and puts.
- `large_trader_futures_tx.parquet`: TX+MTX/4+TMF/20 top-five and top-ten long
  and short concentration, including the specific-corporate values in
  parentheses.
- `taifex_vix_daily_recent.parquet`: official TAIFEX VIX and its pre-close
  one-minute average for every monthly file still exposed by the rolling page.

## Causal use

TAIFEX positioning and ratio reports are post-close facts. Every normalized row
therefore keeps both `date` and `available_date`; `available_date` is the next
session in the receipt-verified TAIFEX calendar. A missing next session remains
null rather than being guessed from weekdays. Models must join on
`available_date`, never use the same day's value for an intraday decision.

OpenAPI snapshots use `captured_at_utc` as their earliest possible information
time. A current endpoint must not be retrospectively assigned to earlier dates,
even if it describes a contract that existed earlier.

## Irreducible public-data boundaries

- Free institutional and large-trader web queries have a rolling history
  window. Older official trade, order and disclosure files require a TAIFEX
  historical-data application; the collector does not fabricate that history.
- Historical five-level books cannot be reconstructed from trade prints.
  Shioaji `BidAskFOPv1` books must be captured prospectively.
- OpenAPI current-state tables without a historical endpoint are accumulated
  from the first receipt onward and are labelled as point-in-time snapshots.
- Time-and-sales OpenAPI feeds are delegated to the existing official recent
  tick and Shioaji collectors, avoiding duplicated large raw tapes.
- CFTC Legacy futures-only history before 2000 is filled from the official
  compressed files; 2000 onward remains owned by the existing OpenBB collector.

Official source entry points:

- <https://www.taifex.com.tw/cht/3/dlOptDailyMarketView>
- <https://www.taifex.com.tw/cht/3/pcRatio>
- <https://www.taifex.com.tw/cht/3/futContractsDate>
- <https://www.taifex.com.tw/cht/3/largeTraderFutQry>
- <https://openapi.taifex.com.tw/swagger.json>
- <https://www.taifex.com.tw/cht/3/hisAppForm>

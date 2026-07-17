# Taiwan execution modes

The backtest has three explicit accounting contracts. Changing a mode changes
execution, fees, settlement state, admissible data, and final artifacts; it is
not a reporting-only switch.

| Mode | Position unit | Holding period | Settlement |
|---|---:|---|---|
| `naive` | continuous weight | existing project behavior | immediate scalar-fee accounting |
| `tw_cash` | long: 1 share; short: 1,000 shares by default | close-to-close cash longs plus optional margin shorts | cash claims, locked short collateral, and margin obligations settle after 2 exchange sessions |
| `tw_day_trade` | 1,000 shares by default | open entry and mandatory same-session close | the round trip's net profit/loss settles after 2 exchange sessions |

## Official timing and fee assumptions

`T+2` means two observed exchange sessions, not two calendar days. A trade on
session `t` enters queue slot 2 and advances only on real sessions. TWSE states
that the investor must complete a net payment by 10:00 on `t+2`; a net receipt
is generally credited after 11:00, not after the close. The simulator resolves
that ordered settlement phase before new trading. A failed delivery is
absorbing; a later signal cannot recreate capital.

The commission reference configured here is `0.001425`, not `0.001455`.
TWSE Rule 94 lets brokers set their own rate, discount, and per-order minimum;
`0.001425` is the threshold above which advance notice and a retained record
are required, not a hard statutory maximum. The configured six-tenths broker
discount produces `0.000855` on each executed side. Tax is sell-only:

| Contract | Buy | Sell |
|---|---:|---:|
| `tw_cash`, stock | 0.0855% | 0.0855% + 0.3% = 0.3855% |
| `tw_cash`, ETF | 0.0855% | 0.0855% + 0.1% = 0.1855% |
| `tw_day_trade`, stock | 0.0855% | 0.0855% + 0.15% = 0.2355% |
| `tw_day_trade`, ETF | 0.0855% | 0.0855% + 0.1% = 0.1855% |

`tw_cash` with `long_only: false` is a hybrid cash/margin account. A positive
target is an owned cash-market holding. A negative target is a separate margin
short (融券) liability; the executor must never represent it by making the cash
share inventory negative. Opening and covering a margin short both pay the
configured commission. The opening sale also pays the ordinary 0.3% stock or
0.1% ETF sell tax shown above; it is not automatically eligible for the 0.15%
same-day stock tax.

Margin shorts use a separate 1,000-share default lot. Exceptional product units
must come from point-in-time security data rather than changing the one-share
cash-long unit. On `T+2`, the opening short-sale proceeds are locked as
collateral instead of becoming spendable cash, and the account must deliver the
required initial margin. Covering releases the attributable locked proceeds and
margin only through settlement; a deficit remains a `T+2` payable.

The current market-wide initial-margin floor is 90%, the maintenance-call
threshold is 130%, and recovery to 166% can cure the call under the current
rules. The executor uses 130% for the deterministic rule that a partial cover
may release collateral only to the extent the remaining short account stays at
or above that ratio. Automatic call notification, client top-ups, and broker
liquidation after the statutory cure window are not inferred from daily OHLC.
The effective-date helper also records these market-wide temporary increases
and their first restoration dates:

- 120% from 2015-08-13 through 2015-10-15; restored to 90% on 2015-10-16;
- 120% from 2016-01-08 through 2016-02-29; restored to 90% on 2016-03-01;
- 120% from 2022-10-12 through 2023-02-23; restored to 90% on 2023-02-24; and
- 130% from 2025-04-07 through 2025-05-25; restored to 90% on 2025-05-26.

Those scalar floors are not a complete shortability schedule. An exchange or
broker may impose a higher security-specific rate, and actual borrow inventory
is broker-specific. Execution must take the maximum with a point-in-time
`[session, symbol]` margin-rate tensor and independently require shortability,
inventory, suspension, price-floor, and mandatory-cover data. It must not infer
that a symbol is shortable merely because the market-wide floor is known.

The opening short handling fee is an explicit broker-profile input. Negotiated
borrow costs and interest credited on locked collateral need lot-age queues,
calendar-day accrual, and broker-specific source data. They are not exposed as
apparently functional annual-rate settings until that accounting exists.

The stock day-trade tax reduction currently runs through 2027-12-31. The
schedule is intentionally configurable and is a current-rate counterfactual
when applied to history; it does not pretend that tax law and broker discounts
were unchanged for every historical date.

Minimum commission and whole-TWD rounding are broker/channel-specific, so the
neutral defaults are `0` and `none`. The exact integer executor supports
`none`, `floor`, and `half_up`. For every nonzero session-symbol-side aggregate
order, it computes the proportional commission and tax separately, applies
their configured rounding, and then applies the commission minimum. A zero
order never incurs a minimum.

Official references:

- [TWSE settlement operations](https://www.twse.com.tw/zh/clearing/clearing/operations.html)
- [TWSE trading and settlement overview](https://www.twse.com.tw/zh/products/system/trading.html)
- [TWSE clearing and netting](https://www.twse.com.tw/zh/clearing/clearing/features.html)
- [TWSE day-trading rules](https://www.twse.com.tw/zh/products/system/day-trading.html)
- [TWSE ETF trading rules](https://www.twse.com.tw/zh/products/securities/etf/overview/rules.html)
- [Securities Transaction Tax Act](https://law-out.mof.gov.tw/LawContent.aspx?id=FL006079)
- [TWSE broker commission rule](https://twse-regulation.twse.com.tw/TW/law/DOC01.aspx?FLCODE=FL007304&FLNO=94)
- [TWSE margin purchase and short sale rules](https://twse-regulation.twse.com.tw/TW/law/DAT0201_print.aspx?FLCODE=FL007121)
- [TWSE Article 56 partial-cover collateral retention](https://twse-regulation.twse.com.tw/TW/law/DOC01.aspx?FLCODE=FL007121&FLNO=56)
- [2016 restoration of the margin-short floor to 90%](https://www.fsc.gov.tw/ch/home.jsp?dataserno=201602260001&dtable=NewsLaw&id=128&mcustomize=lawnew_view.jsp&parentpath=0%2C3&toolsflag=Y)
- [2022 temporary 120% margin order](https://law.fsc.gov.tw/LawContent.aspx?id=GL003521)
- [Current 90% initial short-margin order, effective 2025-05-26](https://twse-regulation.twse.com.tw/TW/int/DAT01.aspx?FLCODE=FE390238)
- [2025 temporary 130% margin order](https://twse-regulation.twse.com.tw/TW/int/DAT01_print.aspx?FLCODE=FE389068)
- [MOPS market-year dividend allocation report](https://mopsov.twse.com.tw/mops/web/t108sb27)

## First-principles ledger

At every valuation boundary, a long-only cash account obeys

```text
NAV = settled_cash + risky_market_value + receivables - payables
```

With margin shorts enabled, the same conservation law expands rather than
turning owned shares negative:

```text
NAV = settled_cash + long_market_value
    + locked_short_proceeds + locked_short_margin
    + receivables - payables
    - short_market_value - accrued_short_costs
```

A buy creates market value and a future payable; a sell removes market value
and creates a future receivable. Fees reduce NAV on the trade date even though
cash delivery occurs later. The trade date produces one account-level net
claim, matching Taiwan clearing netting:

```text
settlement_net = net_sell_proceeds - gross_buy_cost
```

For a day-trade long of `q` shares:

```text
settlement_net = q * close * (1 - sell_fee) - q * open * (1 + buy_fee)
```

For a sell-first day trade, open and close switch sides. Both variants are flat
at the session boundary. If the opening leg fills but the mandatory closing
side is unavailable, daily OHLC data cannot price the unresolved position, so
the executor fails closed instead of cancelling the already-observed opening
fill with hindsight.

## Corporate actions

Cash execution uses raw close-to-next-close returns because adjusted total
returns would silently fabricate fractional shares or immediate dividend
reinvestment. Two explicit contracts are available.

`tw_corporate_action_mode: avoid` maps the receipt-verified official
corporate-action archive to the last two-sided executable exchange close before
each declared ex-date. From that close through the last cum-right close the
executor:

1. blocks a new position;
2. liquidates an existing position and charges the normal sell fee;
3. bypasses voluntary turnover and volume caps for the required exit;
4. fails closed if the mandatory side is unavailable; and
5. permits re-entry after the transition.

`tw_corporate_action_mode: exact` joins every exchange event to an immutable,
SHA-256-bound MOPS issuer receipt. The v3 archive also binds every POST URL and
request body to the exact market-year bulk response in a sorted, content-addressed
receipt manifest; the panel refuses a missing, modified, or row-count-mismatched
manifest. The official `ajax_t108sb27` report covers one market-year per request,
so 2005--2026 needs 44 receipts rather than tens of thousands of issuer queries.
Its three historical table layouts are parsed under an explicit contract;
same-event corrections select the latest official announcement time, while a
conflicting tied revision fails closed. A pure cash event is exact only when
cash per share, payment date, and the absence of stock/subscription terms are
all proven. Stop-transfer data is a separate short-side input and is not a
precondition for an exact long cash claim.
A long held at the final cum-dividend close earns a receivable immediately;
selling later cannot erase it, and settled cash changes only on the announced
payment session. The raw ex-date price drop remains in the asset return. The
receivable queue is deliberately longer than T+2 and its configured horizon
must cover every announced delay.

When an issuer receipt supplies a stop-transfer start, it independently drives
the margin-short Article 76 contract: cover by the sixth preceding exchange
business day and block new margin short sales for four sessions from that
deadline. The Lunar New Year
exception is calendar-aware: depending on the stop-transfer date, one or both
settlement-only days after the final pre-holiday trade count as business days.
If the legal deadline has no executable buy side, the cover moves to the last
earlier executable close and re-opening is blocked from that effective cover;
otherwise a newly opened liability could cross stop-transfer without a second
mandatory-cover event. If no earlier executable buy exists at all, the legal
deadline remains an explicit forced-cover row so a carried short fails closed
instead of making the Article 76 event disappear.
Complex, incomplete, ETF, or other unsupported events stay on the conservative
avoidance path. When such an event has an exchange ex-date but no MOPS
stop-transfer term, margin shorts use the conservative ordinary T+2 fallback:
cover three sessions before the final cum-right close and block re-opening
through that close. It deliberately does not apply the Lunar New Year
relaxation without issuer evidence, so the fallback cannot move a legal cover
later. The old adjusted-versus-rounded-price heuristic is not used.

The receipt-backed 2005-01-01 through 2026-07-11 baseline contains 31,316
exchange events. Of 18,306 listed-company cash reference events, 13,691 are
exact cash claims, 4,615 remain on the complex/incomplete avoidance path, and
810 of those have no matching MOPS bulk row. The 44-response manifest and
canonical parquet both carry SHA-256 receipts; an incomplete rebuild cannot
replace them.

Exact cash claims use the same bounded recurrent CUDA compiler as ordinary
`tw_cash`. The outer loss deliberately stays eager so it cannot unroll the
whole training horizon into a graph proportional to `T`; only a fixed
`tw_continuous_compile_chunk_rows` block is compiled and its differentiable
cash/payable/receivable/collateral state is chained across chunks. On the
measured RTX 5070 Ti shape `T=32, S=2304, Q=256, chunk=4`, exact forward plus
backward fell from about `1679.5 ms` eager to `36.5 ms` steady-state compiled;
the bounded cold compile completed in `44.1 s`, while the old full-horizon
experiment was still in Inductor scheduling after two minutes. Reproduce with
`scripts/benchmark_tw_execution.py`.

A production-contract chunk sweep (cash longs, margin shorts, forced covers,
exact dividends, and backward all enabled) measured the following on the same
`T=32, S=2304, Q=256` shape. Every candidate produced the same scalar value and
maximum absolute gradient with zero eager fallbacks:

| chunk rows | cold compile + backward | steady median forward + backward |
| ---: | ---: | ---: |
| 1 | 19.3 s | 82.6 ms |
| 2 | 25.7 s | 50.3 ms |
| 4 | 43.1 s | 38.4 ms |
| 8 | 95.1 s | 35.6 ms |

The active value remains `4`: chunk 8 saves only about 2.8 ms per training
batch but adds about 52 s of cold compilation. At the active batch size 128 it
normally needs hundreds of epochs to recover that startup cost, after the
likely early-stopping horizon. Chunk 4 also overtakes chunk 2 after roughly a
few dozen epochs for a full 2005--2024 train group, so it is the measured
end-to-end wall-clock optimum rather than merely the fastest steady kernel.

## Point-in-time day-trade rules

The downloader stores the exact-session TWSE and TPEx eligibility lists and
sell-first suspension markers from 2014-01-06 onward. Each daily receipt is
validated for date identity, schema, row width, declared row count, unique
symbols, and known suspension markers. Feature construction joins those member
lists against the receipt-certified daily venue universe, producing explicit
0/1 membership rather than carrying today's list backward.

Signals for both Taiwan modes use features only through `t-1`. Cash orders then
execute at close `t`; day trades enter at open `t`. Open-side price-limit masks
are derived from the prior close and are kept separate from close-side masks.
The model's day-trade outer mask contains only point-in-time eligibility. Actual
open availability is applied later by the executor and must not reselect or
renormalize the signal at the same price where it claims a fill. It never
exposes same-day open, close, return, or full-day-volume availability.

Liquidity caps for both Taiwan modes use the previous completed session's
share volume. The executor values that causal share reference at today's
execution proxy only to convert it into portfolio-weight units: current close
for `tw_cash`, and current open for `tw_day_trade`. The public integer API
therefore requires `cash_close_volume_reference` for cash orders and
`day_trade_entry_volume_reference` for day-trade entry orders whenever
participation limiting is enabled. Same-row `daily_volumes` is deliberately
rejected for either mode because it contains information from the session,
including the closing auction, that was not available when the signal was
formed.

## Differentiable and exact paths

Training and validation use an FP32 continuous settlement surrogate. Its time
recurrence is inherently sequential, while all symbols are vectorized, giving
`O(T*S)` time and `O(S + settlement_lag)` recurrent state—the lower asymptotic
bound when every order and return must be inspected. Broker minimums, currency
rounding, and integer lots remain outside this differentiable path.

Final Taiwan test artifacts are replayed by the float64/integer oracle:

- `tw_cash`: exact share lots, cash affordability, fees, holdings, and T+2 queues;
- `tw_day_trade`: exact 1,000-share lots, open/close legs, fees, and T+2 net P&L;
- `test_backtest.npz`: canonical integer result with ledger unit `currency`;
- `test_backtest_continuous_surrogate.npz`: training surrogate with ledger unit `nav_ratio`;
- settlement CSV/JSON: complete due-slot queue history, not only queue totals.

Backtest artifact schema 4 records payable and receivable queue horizons
independently. Ordinary trade payables remain `T+2`, while exact dividend
receivables use the configured longer claim horizon (256 sessions in the active
config). Both lengths are stored and validated on reload; settlement audit
tables emit every column from each queue instead of truncating the longer one.

Pending claims at the finite test boundary are retained in final state rather
than accelerated or discarded.

The continuous path carries an explicit `equity_scale` state. Participation
limits are specified against the configured account equity, so the same share
volume permits fewer portfolio-weight units after the strategy grows and more
after it shrinks. This scale is serialized and carried across chunks; resetting
it at a batch boundary would change fills and is therefore forbidden.

Cash valuation distinguishes an executable quote from a mark. During an
ordinary halt, the raw missing close cannot be used for a trade, but an existing
holding is marked at its last official close. When trading resumes, the full
price jump is recognized exactly once. The stale mark is cleared after a forced
exit, lifecycle break, or corporate action, so it cannot bridge two different
security lives. At the finite panel horizon, a reconstructable final mark has a
zero next-session price return; fees, holdings, and unsettled claims remain in
the terminal ledger instead of being erased by a missing future label.

Walk-forward deployment is also one accounting path, not a concatenation of
independent fold NAV curves. Fold requests are expanded into the immutable full
panel symbol order and replayed once over strictly contiguous exchange sessions.
Holdings, settled cash, T+2 queues, defaults, and the absorbing alive state thus
cross model hand-offs. Per-fold deployment files are slices of that canonical
replay; the root `walkforward_deployment_backtest.npz` is the authoritative
stitched result.

## Configuration

The existing behavior remains the default:

```yaml
trading:
  execution_mode: naive
```

Cash shares without shorting:

```yaml
trading:
  execution_mode: tw_cash
  long_only: true
  reporting_leverage: 1.0
  tw_cash_lot_size: 1
  tw_settlement_lag_sessions: 2
```

Cash longs plus margin shorts:

```yaml
trading:
  execution_mode: tw_cash
  long_only: false
  reporting_leverage: 1.0
  tw_cash_lot_size: 1
  tw_short_lot_size: 1000
  tw_short_initial_margin_rate: 0.9
  tw_short_maintenance_ratio: 1.3
  tw_short_handling_fee_rate: 0.0
  tw_short_capacity_limit_enabled: true
  tw_corporate_action_mode: exact
  tw_corporate_action_claim_queue_sessions: 256
  tw_settlement_lag_sessions: 2
```

When broker-specific historical borrow inventory is unavailable, an explicit
counterfactual run may set `tw_short_capacity_limit_enabled: false`. This
removes only the demonstrated-share ceiling. Point-in-time margin eligibility,
exchange/broker bans represented by the rule feed, mandatory covers, margin,
collateral, fees, lot size, and T+2 settlement remain active. The default is
`true`; the switch is part of the semantic checkpoint contract.

Same-day round trips:

```yaml
trading:
  execution_mode: tw_day_trade
  long_only: false
  reporting_leverage: 1.0
  tw_day_trade_lot_size: 1000
  tw_settlement_lag_sessions: 2
```

Optional broker profile example:

```yaml
trading:
  tw_minimum_commission: 20.0
  tw_commission_rounding: floor
  tw_tax_rounding: floor
```

The Taiwan execution schedule is included in the semantic checkpoint
fingerprint. A checkpoint with a different accounting contract may be used for
inference only where compatibility policy permits; it must not silently resume
an optimizer trajectory under changed trading semantics.

## Deliberate limitations

- Daily OHLC uses the reported open/close as execution proxies; it does not
  model intraday queue priority, slippage, auction microstructure, or partial
  fills beyond configured volume participation.
- One-share cash execution models quantity accurately but not the potentially
  different odd-lot auction price.
- Market-wide short-margin dates do not replace point-in-time per-security
  rates, shortability, broker inventory, price restrictions, margin calls, or
  mandatory corporate-action and delisting covers. Missing execution inputs
  fail closed. The 130% partial-cover retention rule is modeled; the later
  notification/top-up/liquidation policy is not, because it depends on external
  cash decisions and broker disposition fills absent from the panel.
- Broker-specific short handling is configurable. Borrow-source costs and
  collateral interest are not silently treated as zero-cost facts; they remain
  outside the result unless a future point-in-time accrual contract is added.
- The default ETF tax profile is the general 0.1% sell tax. Qualifying
  non-leveraged/non-inverse bond ETFs are tax-exempt through 2026-12-31, but the
  current symbol-only classifier cannot prove that product subtype and therefore
  does not silently grant the exemption.
- Day-trade buying power/margin limits beyond cash settlement default are not
  inferred because no broker credit contract was specified.
- Bond-ETF exemptions, special products, warrants, rights, units, preferreds,
  ETNs, and debt instruments are not guessed into the stock/ETF schedule.
- Live order placement for non-naive modes remains fail-closed until a broker
  adapter can carry exact lots and settlement state; backtest/report support
  does not masquerade as live execution support.
